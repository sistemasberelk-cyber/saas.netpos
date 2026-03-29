import barcode
from barcode.writer import ImageWriter
from sqlmodel import Session, select
from database.models import Product, Sale, SaleItem, User, Payment
from typing import List, Optional
import os
from datetime import datetime

class StockService:
    def __init__(self, static_dir: str = "static/barcodes"):
        self.static_dir = static_dir
        os.makedirs(self.static_dir, exist_ok=True)

    def generate_barcode(self, product_id: int) -> str:
        """
        Generates a barcode for a product_id. 
        Returns the filename of the generated barcode image.
        Format: EAN13 (or Code128 if preferred).
        """
        # Switch to SVG for perfect scalability and print quality
        # No writer specified = Default SVGWriter (vectors)
        code = barcode.get('code128', str(product_id).zfill(8))
        filename = f"product_{product_id}"
        full_path = os.path.join(self.static_dir, filename)
        code.save(full_path) # saves as filename.svg
        return f"{filename}.svg"

    def process_sale(self, session: Session, user_id: int, tenant_id: int, items_data: List[dict], payment_method: str = "cash", client_id: Optional[int] = None, amount_paid: Optional[float] = None) -> Sale:
        """
        Creates a Sale record and updates product stock.
        If client_id is provided and amount_paid > 0, creates a Payment record.
        items_data expected format: [{"product_id": 1, "quantity": 2}, ...]
        """
        sale = Sale(tenant_id=tenant_id, user_id=user_id, payment_method=payment_method, client_id=client_id, timestamp=datetime.now())
        total_sale = 0.0
        
        for item in items_data:
            p_id = item["product_id"]
            qty = item["quantity"]
            
            # Verify product belongs to tenant
            product = session.exec(select(Product).where(Product.id == p_id, Product.tenant_id == tenant_id)).first()
            if not product:
                raise ValueError(f"Product {p_id} not found or access denied")
            
            if product.stock_quantity < qty:
                raise ValueError(f"Insufficient stock for {product.name}")
            
            # --- Credit Limit Check ---
            # If sale is not fully paid (Current Account), check limit
            # Calculate what part is debt
            # Determine Price: Always prefer Bulk Price if available
            unit_price = product.price_bulk if (product.price_bulk and product.price_bulk > 0) else product.price

            # Decrement Stock
            product.stock_quantity -= qty
            session.add(product)

            # Create Sale Item
            line_total = unit_price * qty
            total_sale += line_total
            
            sale_item = SaleItem(
                product_id=p_id,
                product_name=product.name,
                quantity=qty,
                unit_price=unit_price,
                total=line_total
            )
            sale.items.append(sale_item)
            
        sale.total_amount = total_sale
        
        # Payment Logic
        final_amount_paid = amount_paid if amount_paid is not None else total_sale
        
        # --- Credit Limit Check ---
        client = None
        if client_id:
            from database.models import Client
            from sqlalchemy import func
            client = session.get(Client, client_id)
            
        if client and final_amount_paid < total_sale:
            if client.tenant_id == tenant_id and client.credit_limit:
                 # Calculate current balance (Debt - Paid) for this tenant
                 
                 # Sum previous sales total
                 stmt_sales = select(func.sum(Sale.total_amount)).where(Sale.client_id == client_id, Sale.tenant_id == tenant_id)
                 current_debt = session.exec(stmt_sales).one() or 0.0
                 
                 # Sum payments
                 stmt_payments = select(func.sum(Payment.amount)).where(Payment.client_id == client_id, Payment.tenant_id == tenant_id)
                 current_paid = session.exec(stmt_payments).one() or 0.0
                 
                 current_balance = current_debt - current_paid
                 new_debt = total_sale - final_amount_paid
                 
                 if (current_balance + new_debt) > client.credit_limit:
                     raise ValueError(f"Credit Limit Exceeded. Limit: ${client.credit_limit}, Current Balance: ${current_balance}, New Debt: ${new_debt}")

        # Determine Status
        if final_amount_paid >= total_sale:
            sale.payment_status = "paid"
        elif final_amount_paid > 0:
            sale.payment_status = "partial"
        else:
            sale.payment_status = "pending"
            
        sale.amount_paid = final_amount_paid
        
        session.add(sale)
        
        # Handle Payment if Client is selected
        if client_id and final_amount_paid > 0:
            # Create a payment record linked to this sale (conceptually via time/client)
            payment = Payment(
                tenant_id=tenant_id,
                client_id=client_id,
                amount=final_amount_paid,
                date=datetime.now(),
                note=f"Pago inmediato en Venta" 
            )
            session.add(payment)
            
        # Register in Cash Book
        if final_amount_paid > 0 and payment_method != "cuenta_corriente":
            from database.models import CashMovement
            client_name = client.name if client else "Consumidor Final"
            cash_movement = CashMovement(
                tenant_id=tenant_id,
                user_id=user_id,
                amount=final_amount_paid,
                movement_type="in",
                concept=f"Ingreso por Venta a {client_name} - Medio: {payment_method}",
                reference_type="sale"
            )
            session.add(cash_movement)

        session.commit()
        session.refresh(sale)
        return sale
