import barcode
from barcode.writer import ImageWriter
from sqlmodel import Session, select
from database.models import Product, Sale, SaleItem, User, Payment, CashMovement
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

    def process_sale(self, session: Session, user_id: int, tenant_id: int, items_data: List[dict], payment_method: str = "cash", client_id: Optional[int] = None, amount_paid: Optional[float] = None, split_cash: Optional[float] = None, split_transfer: Optional[float] = None) -> Sale:
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
            
            current_stock = self.get_total_stock(session, product.id, tenant_id)
            if current_stock < qty:
                raise ValueError(f"Insufficient stock for {product.name}")
            
            # Determine Price: Explicitly honor price_type if sent from POS
            price_type = item.get("price_type")
            if price_type == "bulk" and product.price_bulk and product.price_bulk > 0:
                unit_price = product.price_bulk
            elif price_type == "retail" and product.price_retail and product.price_retail > 0:
                unit_price = product.price_retail
            elif product.price_bulk and product.price_bulk > 0 and product.cant_bulto and qty >= product.cant_bulto:
                # Fallback to automatic logic if no type or type is "unit" but quantity meets bulk threshold
                unit_price = product.price_bulk
            else:
                unit_price = product.price


            # Decrement Stock
            self.add_stock(session, p_id, tenant_id, -qty, "venta", "Salida por venta", user_id)

            # Create Sale Item (with cost snapshot for profitability)
            line_total = unit_price * qty
            total_sale += line_total
            
            sale_item = SaleItem(
                product_id=p_id,
                product_name=product.name,
                quantity=qty,
                unit_price=unit_price,
                total=line_total,
                cost_price_at_sale=product.cost_price or 0.0
            )
            sale.items.append(sale_item)
            
        sale.total_amount = total_sale
        
        # Payment Logic
        if split_cash is not None or split_transfer is not None:
            amt_cash = split_cash or 0.0
            amt_transfer = split_transfer or 0.0
            final_amount_paid = amt_cash + amt_transfer
            if amt_cash > 0 and amt_transfer == 0:
                sale.payment_method = "cash"
            elif amt_transfer > 0 and amt_cash == 0:
                sale.payment_method = "transfer"
            elif amt_cash == 0 and amt_transfer == 0:
                sale.payment_method = "cuenta_corriente"
            else:
                sale.payment_method = "combinado"
        else:
            final_amount_paid = amount_paid if amount_paid is not None else total_sale
            amt_cash = final_amount_paid if payment_method == "cash" else 0.0
            amt_transfer = final_amount_paid if payment_method == "transfer" else 0.0
            sale.payment_method = payment_method
        
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
        
        from database.models import PaymentAllocation
        if amt_cash > 0:
            sale.payment_allocations.append(PaymentAllocation(method="cash", amount=amt_cash))
        if amt_transfer > 0:
            sale.payment_allocations.append(PaymentAllocation(method="transfer", amount=amt_transfer))
        
        session.add(sale)
        session.flush()
        
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
        if final_amount_paid > 0:
            from database.models import CashMovement
            client_name = client.name if client else "Consumidor Final"
            
            if amt_cash > 0:
                session.add(CashMovement(
                    tenant_id=tenant_id, user_id=user_id, amount=amt_cash,
                    movement_type="in", concept=f"Ingreso por Venta a {client_name} - Medio: Efectivo",
                    reference_type="sale", reference_id=sale.id
                ))
            if amt_transfer > 0:
                session.add(CashMovement(
                    tenant_id=tenant_id, user_id=user_id, amount=amt_transfer,
                    movement_type="in", concept=f"Ingreso por Venta a {client_name} - Medio: Transferencia",
                    reference_type="sale", reference_id=sale.id
                ))

        session.commit()
        return sale

    def get_total_stock(self, session: Session, product_id: int, tenant_id: int) -> int:
        from database.models import BinStock
        from sqlalchemy import func
        total = session.exec(select(func.sum(BinStock.quantity)).where(BinStock.product_id == product_id, BinStock.tenant_id == tenant_id)).one()
        return total or 0

    def add_stock(self, session: Session, product_id: int, tenant_id: int, quantity: int, reason: str, notes: str, user_id: int = None):
        if quantity == 0: return
        from services.bin_stock_service import BinStockService
        from database.models import Bin
        bin_ = session.exec(select(Bin).where(Bin.tenant_id == tenant_id, Bin.name == "SIN-UBICACION")).first()
        if not bin_:
            bin_ = session.exec(select(Bin).where(Bin.tenant_id == tenant_id, Bin.is_active == True)).first()
        if not bin_:
            raise ValueError("No hay ubicaciones configuradas para descontar stock.")
        
        from database.models import BinStock
        current_bs = session.exec(select(BinStock).where(BinStock.bin_id == bin_.id, BinStock.product_id == product_id)).first()
        current_qty = current_bs.quantity if current_bs else 0
        new_qty = current_qty + quantity
        BinStockService.adjust_stock(session, tenant_id, bin_.id, product_id, new_qty, reason, notes, user_id)
