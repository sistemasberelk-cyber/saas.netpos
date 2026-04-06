"""
services/bin_stock_service.py
==============================
Servicio transaccional para operaciones de stock por posición.
Toda lógica de negocio de WMS vive aquí — el router solo delega.
"""

from datetime import datetime, timezone
from typing import Optional
from sqlmodel import Session, select
from sqlalchemy import func

from database.models import (
    Bin, BinStock, StockMovement, Product, Location
)


class StockServiceError(Exception):
    """Error de dominio del servicio de stock."""
    def __init__(self, message: str, status_code: int = 400):
        self.message = message
        self.status_code = status_code
        super().__init__(message)


class BinStockService:
    """Operaciones transaccionales de stock por posición."""

    @staticmethod
    def _get_bin_or_raise(session: Session, bin_id: int, tenant_id: int) -> Bin:
        bin_ = session.get(Bin, bin_id)
        if not bin_ or bin_.tenant_id != tenant_id:
            raise StockServiceError(f"Ubicación {bin_id} no encontrada", 404)
        return bin_

    @staticmethod
    def _get_product_or_raise(session: Session, product_id: int, tenant_id: int) -> Product:
        product = session.get(Product, product_id)
        if not product or product.tenant_id != tenant_id:
            raise StockServiceError(f"Producto {product_id} no encontrado", 404)
        return product

    @staticmethod
    def adjust_stock(
        session: Session,
        tenant_id: int,
        bin_id: int,
        product_id: int,
        new_quantity: int,
        reason: str = "ajuste",
        notes: Optional[str] = None,
        user_id: Optional[int] = None,
    ) -> dict:
        """
        Ajusta stock en una posición a una cantidad final.
        Sincroniza product.stock_quantity en la misma transacción.
        """
        if new_quantity < 0:
            raise StockServiceError("La cantidad no puede ser negativa")

        bin_ = BinStockService._get_bin_or_raise(session, bin_id, tenant_id)
        product = BinStockService._get_product_or_raise(session, product_id, tenant_id)

        if bin_.max_capacity is not None and new_quantity > bin_.max_capacity:
            raise StockServiceError(
                f"Excede la capacidad máxima de esta ubicación ({bin_.max_capacity})"
            )

        # Buscar o crear BinStock
        bin_stock = session.exec(
            select(BinStock).where(
                BinStock.bin_id == bin_id,
                BinStock.product_id == product_id
            )
        ).first()

        old_qty = bin_stock.quantity if bin_stock else 0
        delta = new_quantity - old_qty

        if bin_stock:
            bin_stock.quantity = new_quantity
            bin_stock.updated_at = datetime.now(timezone.utc)
        else:
            bin_stock = BinStock(
                tenant_id=tenant_id,
                bin_id=bin_id,
                product_id=product_id,
                quantity=new_quantity,
            )
        session.add(bin_stock)

        # Auditoría y sincronización solo si hubo cambio
        if delta != 0:
            movement = StockMovement(
                tenant_id=tenant_id,
                product_id=product_id,
                from_bin_id=None if delta > 0 else bin_id,
                to_bin_id=bin_id if delta > 0 else None,
                quantity=abs(delta),
                reason=reason,
                notes=notes,
                user_id=user_id,
            )
            session.add(movement)

            # Sincronizar stock global
            product.stock_quantity = max(0, product.stock_quantity + delta)
            session.add(product)

        session.commit()
        return {
            "ok": True,
            "bin_id": bin_id,
            "product_id": product_id,
            "old_quantity": old_qty,
            "new_quantity": new_quantity,
            "delta": delta,
        }

    @staticmethod
    def transfer_stock(
        session: Session,
        tenant_id: int,
        product_id: int,
        from_bin_id: int,
        to_bin_id: int,
        quantity: int,
        notes: Optional[str] = None,
        request_id: Optional[str] = None,
        user_id: Optional[int] = None,
    ) -> dict:
        """
        Transfiere stock entre dos posiciones con lock pesimista.
        Idempotente si se provee request_id.
        """
        if quantity <= 0:
            raise StockServiceError("La cantidad a transferir debe ser mayor a 0")
        if from_bin_id == to_bin_id:
            raise StockServiceError("Origen y destino no pueden ser iguales")

        # Idempotencia
        if request_id:
            existing = session.exec(
                select(StockMovement).where(StockMovement.request_id == request_id)
            ).first()
            if existing:
                return {"ok": True, "idempotent": True, "movement_id": existing.id}

        from_bin = BinStockService._get_bin_or_raise(session, from_bin_id, tenant_id)
        to_bin = BinStockService._get_bin_or_raise(session, to_bin_id, tenant_id)

        # Lock pesimista sobre BinStock origen
        from_stock = session.exec(
            select(BinStock).where(
                BinStock.bin_id == from_bin_id,
                BinStock.product_id == product_id,
            ).with_for_update()
        ).first()

        if not from_stock or from_stock.quantity < quantity:
            available = from_stock.quantity if from_stock else 0
            raise StockServiceError(
                f"Stock insuficiente en origen. Disponible: {available}", 409
            )

        # Lock pesimista sobre destino
        to_stock = session.exec(
            select(BinStock).where(
                BinStock.bin_id == to_bin_id,
                BinStock.product_id == product_id,
            ).with_for_update()
        ).first()

        to_current = to_stock.quantity if to_stock else 0
        if to_bin.max_capacity is not None and (to_current + quantity) > to_bin.max_capacity:
            raise StockServiceError(
                f"Excede la capacidad máxima del destino ({to_bin.max_capacity})", 409
            )

        # Ejecutar transferencia atómica
        now = datetime.now(timezone.utc)
        from_stock.quantity -= quantity
        from_stock.updated_at = now
        session.add(from_stock)

        if to_stock:
            to_stock.quantity += quantity
            to_stock.updated_at = now
            session.add(to_stock)
        else:
            session.add(BinStock(
                tenant_id=tenant_id,
                bin_id=to_bin_id,
                product_id=product_id,
                quantity=quantity,
            ))

        movement = StockMovement(
            tenant_id=tenant_id,
            product_id=product_id,
            from_bin_id=from_bin_id,
            to_bin_id=to_bin_id,
            quantity=quantity,
            reason="transferencia",
            notes=notes,
            request_id=request_id,
            user_id=user_id,
        )
        session.add(movement)
        # Stock global no cambia en transferencias (misma cantidad distribuida)
        session.commit()

        return {
            "ok": True,
            "movement_id": movement.id,
            "from_bin_id": from_bin_id,
            "to_bin_id": to_bin_id,
            "quantity": quantity,
        }

    @staticmethod
    def reconcile_product(session: Session, tenant_id: int, product_id: int) -> dict:
        """
        Compara product.stock_quantity con SUM(bin_stock).
        Retorna la diferencia. Con fix=True, alinea el stock global.
        """
        product = session.get(Product, product_id)
        if not product or product.tenant_id != tenant_id:
            raise StockServiceError("Producto no encontrado", 404)

        bin_total = session.exec(
            select(func.sum(BinStock.quantity)).where(
                BinStock.product_id == product_id,
                BinStock.tenant_id == tenant_id,
            )
        ).one() or 0

        diff = product.stock_quantity - bin_total
        return {
            "product_id": product_id,
            "product_name": product.name,
            "stock_global": product.stock_quantity,
            "stock_en_posiciones": bin_total,
            "diferencia": diff,
            "ok": diff == 0,
        }

    @staticmethod
    def reconcile_all(session: Session, tenant_id: int, fix: bool = False) -> list:
        """Reconcilia todos los productos del tenant. Si fix=True, corrige stock global."""
        products = session.exec(
            select(Product).where(Product.tenant_id == tenant_id)
        ).all()

        results = []
        for p in products:
            r = BinStockService.reconcile_product(session, tenant_id, p.id)
            if not r["ok"]:
                if fix:
                    p.stock_quantity = r["stock_en_posiciones"]
                    session.add(p)
                results.append(r)

        if fix:
            session.commit()

        return results

    @staticmethod
    def backfill_default_location(session: Session, tenant_id: int) -> dict:
        """
        Crea el depósito 'Depósito Central' y bin 'SIN-UBICACION' por defecto.
        Inserta bin_stock inicial con el stock global de cada producto.
        Idempotente — no hace nada si ya existe.
        """
        # Crear depósito default si no existe
        location = session.exec(
            select(Location).where(
                Location.tenant_id == tenant_id,
                Location.code == "DEP-CENTRAL"
            )
        ).first()

        if not location:
            location = Location(
                tenant_id=tenant_id,
                name="Depósito Central",
                code="DEP-CENTRAL",
                description="Depósito principal (creado automáticamente)",
            )
            session.add(location)
            session.flush()

        # Crear bin default
        default_bin = session.exec(
            select(Bin).where(
                Bin.tenant_id == tenant_id,
                Bin.location_id == location.id,
                Bin.name == "SIN-UBICACION"
            )
        ).first()

        if not default_bin:
            default_bin = Bin(
                tenant_id=tenant_id,
                location_id=location.id,
                name="SIN-UBICACION",
                description="Posición por defecto para stock no asignado",
            )
            session.add(default_bin)
            session.flush()

        # Backfill: cada producto va a SIN-UBICACION con su stock global actual
        products = session.exec(
            select(Product).where(Product.tenant_id == tenant_id)
        ).all()

        created = 0
        for p in products:
            existing = session.exec(
                select(BinStock).where(
                    BinStock.bin_id == default_bin.id,
                    BinStock.product_id == p.id
                )
            ).first()
            if not existing and p.stock_quantity > 0:
                session.add(BinStock(
                    tenant_id=tenant_id,
                    bin_id=default_bin.id,
                    product_id=p.id,
                    quantity=p.stock_quantity,
                ))
                created += 1

        session.commit()
        return {
            "ok": True,
            "location_id": location.id,
            "default_bin_id": default_bin.id,
            "products_backfilled": created,
        }
