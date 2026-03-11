from typing import List, Dict, Any, Optional
from sqlmodel import Session, select

from database.models import Product, Purchase, PurchaseItem, Supplier, CashMovement


class PurchaseService:
    @staticmethod
    def create_supplier(session: Session, tenant_id: int, **kwargs) -> Supplier:
        supplier = Supplier(tenant_id=tenant_id, **kwargs)
        session.add(supplier)
        session.commit()
        session.refresh(supplier)
        return supplier

    @staticmethod
    def process_purchase(
        session: Session,
        user_id: int,
        tenant_id: int,
        supplier_id: Optional[int],
        invoice_number: Optional[str],
        items_data: List[Dict[str, Any]],
        amount_paid: float = 0.0,
        cash_concept: str = "Pago de mercaderia",
    ) -> Purchase:
        if not items_data:
            raise ValueError("La compra debe tener al menos un producto")
        if amount_paid < 0:
            raise ValueError("El monto pagado no puede ser negativo")

        purchase = Purchase(
            tenant_id=tenant_id,
            supplier_id=supplier_id,
            invoice_number=(invoice_number or "").strip() or None,
            status="pending",
        )
        session.add(purchase)
        session.flush()

        total_amount = 0.0
        for item_info in items_data:
            product_id = item_info.get("product_id")
            quantity = int(item_info.get("quantity", 0))
            unit_cost = float(item_info.get("unit_cost", 0.0))

            if quantity <= 0:
                raise ValueError("La cantidad debe ser mayor a 0")
            if unit_cost < 0:
                raise ValueError("El costo unitario no puede ser negativo")

            product = session.get(Product, product_id)
            if not product or product.tenant_id != tenant_id:
                raise ValueError(f"Producto ID {product_id} no encontrado")

            item_total = quantity * unit_cost
            total_amount += item_total

            product.cost_price = unit_cost
            product.stock_quantity += quantity
            session.add(product)
            session.add(
                PurchaseItem(
                    purchase_id=purchase.id,
                    product_id=product.id,
                    product_name=product.name,
                    quantity=quantity,
                    unit_cost=unit_cost,
                    total=item_total,
                )
            )

        if amount_paid > total_amount:
            raise ValueError("El monto pagado no puede superar el total de la compra")

        purchase.total_amount = total_amount
        if amount_paid == 0:
            purchase.status = "pending"
        elif amount_paid < total_amount:
            purchase.status = "partial"
        else:
            purchase.status = "paid"
        session.add(purchase)

        if amount_paid > 0:
            session.add(
                CashMovement(
                    tenant_id=tenant_id,
                    amount=-abs(amount_paid),
                    movement_type="out",
                    concept=cash_concept,
                    reference_id=purchase.id,
                    reference_type="purchase",
                    user_id=user_id,
                )
            )

        session.commit()
        session.refresh(purchase)
        return purchase

    @staticmethod
    def get_supplier_balance(session: Session, tenant_id: int, supplier_id: int) -> float:
        purchases = session.exec(
            select(Purchase).where(Purchase.supplier_id == supplier_id, Purchase.tenant_id == tenant_id)
        ).all()
        purchase_ids = [purchase.id for purchase in purchases if purchase.id is not None]

        direct_payments = session.exec(
            select(CashMovement).where(
                CashMovement.tenant_id == tenant_id,
                CashMovement.reference_type == "supplier_payment",
                CashMovement.reference_id == supplier_id,
            )
        ).all()

        purchase_payments = []
        if purchase_ids:
            purchase_payments = session.exec(
                select(CashMovement).where(
                    CashMovement.tenant_id == tenant_id,
                    CashMovement.reference_type == "purchase",
                    CashMovement.reference_id.in_(purchase_ids),
                )
            ).all()

        total_owed = sum(purchase.total_amount for purchase in purchases)
        total_paid = sum(abs(payment.amount) for payment in [*direct_payments, *purchase_payments])
        return float(total_owed - total_paid)

    @staticmethod
    def build_supplier_movements(session: Session, tenant_id: int, supplier_id: int) -> list[dict[str, Any]]:
        purchases = session.exec(
            select(Purchase).where(Purchase.supplier_id == supplier_id, Purchase.tenant_id == tenant_id)
        ).all()
        purchase_map = {purchase.id: purchase for purchase in purchases if purchase.id is not None}

        direct_payments = session.exec(
            select(CashMovement).where(
                CashMovement.tenant_id == tenant_id,
                CashMovement.reference_type == "supplier_payment",
                CashMovement.reference_id == supplier_id,
            )
        ).all()

        purchase_payments = []
        if purchase_map:
            purchase_payments = session.exec(
                select(CashMovement).where(
                    CashMovement.tenant_id == tenant_id,
                    CashMovement.reference_type == "purchase",
                    CashMovement.reference_id.in_(list(purchase_map.keys())),
                )
            ).all()

        movements: list[dict[str, Any]] = []
        for purchase in purchases:
            movements.append(
                {
                    "date": purchase.timestamp,
                    "description": f"Factura/Remito: {purchase.invoice_number or 'N/A'}",
                    "amount": purchase.total_amount,
                    "type": "purchase",
                }
            )

        for payment in direct_payments:
            movements.append(
                {
                    "date": payment.timestamp,
                    "description": f"Pago: {payment.concept or ''}",
                    "amount": abs(payment.amount),
                    "type": "payment",
                }
            )

        for payment in purchase_payments:
            purchase = purchase_map.get(payment.reference_id)
            label = purchase.invoice_number if purchase and purchase.invoice_number else f"Compra #{payment.reference_id}"
            movements.append(
                {
                    "date": payment.timestamp,
                    "description": f"Pago aplicado en compra: {label}",
                    "amount": abs(payment.amount),
                    "type": "payment",
                }
            )

        movements.sort(key=lambda item: item["date"], reverse=True)
        return movements

    @staticmethod
    def register_manual_cash_movement(
        session: Session,
        tenant_id: int,
        user_id: int,
        amount: float,
        movement_type: str,
        concept: str,
        reference_id: Optional[int] = None,
        reference_type: Optional[str] = None,
    ) -> CashMovement:
        final_amt = abs(amount) if movement_type == "in" else -abs(amount)
        movement = CashMovement(
            tenant_id=tenant_id,
            user_id=user_id,
            amount=final_amt,
            movement_type=movement_type,
            concept=concept,
            reference_id=reference_id,
            reference_type=reference_type,
        )
        session.add(movement)
        session.commit()
        session.refresh(movement)
        return movement
