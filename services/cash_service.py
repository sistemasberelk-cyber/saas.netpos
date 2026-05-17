"""services/cash_service.py — Lógica de balance y movimientos de caja"""
from __future__ import annotations
from datetime import datetime, date, timedelta, timezone
from typing import Optional, Dict, Any
from sqlmodel import Session, select, func
from database.models import CashMovement, Sale

class CashService:
    @staticmethod
    def calculate_daily_balance(session: Session, tenant_id: int, target_date: date) -> Dict[str, float]:
        """Calcula el balance diario: ingresos (efectivo/transf), egresos y saldo."""
        day_start = datetime.combine(target_date, datetime.min.time()).replace(tzinfo=timezone.utc)
        day_end = day_start + timedelta(days=1)

        movements = session.exec(
            select(CashMovement)
            .where(
                CashMovement.tenant_id == tenant_id,
                CashMovement.timestamp >= day_start,
                CashMovement.timestamp < day_end,
            )
        ).all()

        def is_close(m):
            return m.movement_type == "cierre" or (m.concept or "").upper().startswith("CIERRE_DE_CAJA")

        last_close_ts = None
        for m in movements:
            if is_close(m) and (last_close_ts is None or m.timestamp > last_close_ts):
                last_close_ts = m.timestamp

        effective = [m for m in movements if not last_close_ts or m.timestamp > last_close_ts]

        total_in_cash = 0.0
        total_in_transfer = 0.0
        total_out = 0.0

        for m in effective:
            amt = m.amount or 0.0
            if amt > 0 and m.movement_type == "in":
                cl = (m.concept or "").lower()
                if "transferencia" in cl or "transfer" in cl:
                    total_in_transfer += amt
                else:
                    total_in_cash += amt
            else:
                total_out += abs(amt)

        # Ventas no registradas en CashMovement
        move_sale_ids = {m.reference_id for m in effective if m.reference_type == "sale" and m.reference_id}
        sales = session.exec(
            select(Sale).where(
                Sale.tenant_id == tenant_id,
                Sale.timestamp >= day_start,
                Sale.timestamp < day_end,
                Sale.amount_paid > 0
            )
        ).all()

        for s in sales:
            if last_close_ts and s.timestamp <= last_close_ts: continue
            if s.id in move_sale_ids: continue
            
            for alloc in s.payment_allocations:
                if alloc.method in ["transfer", "qr"]:
                    total_in_transfer += alloc.amount
                else:
                    total_in_cash += alloc.amount

        return {
            "total_in_cash": total_in_cash,
            "total_in_transfer": total_in_transfer,
            "total_in": total_in_cash + total_in_transfer,
            "total_out": total_out,
            "balance": (total_in_cash + total_in_transfer) - total_out
        }

    @staticmethod
    def perform_cierre(session: Session, tenant_id: int, user_id: int) -> Dict[str, Any]:
        """Ejecuta el cierre de caja, retirando el saldo pendiente."""
        balance_data = CashService.calculate_daily_balance(session, tenant_id, date.today())
        current_balance = balance_data["balance"]
        
        # Marcar ventas como cerradas
        open_sales = session.exec(select(Sale).where(Sale.tenant_id == tenant_id, Sale.is_closed == False)).all()
        for s in open_sales:
            s.is_closed = True
            session.add(s)

        if current_balance > 0.01:
            m = CashMovement(
                tenant_id=tenant_id,
                user_id=user_id,
                amount=current_balance,
                movement_type="out",
                concept=f"CIERRE_DE_CAJA: Retiro de Saldo (${current_balance:.2f})",
                timestamp=datetime.now(timezone.utc)
            )
            session.add(m)
            status = "cierre_con_saldo"
        else:
            m = CashMovement(
                tenant_id=tenant_id,
                user_id=user_id,
                movement_type="cierre",
                amount=0.0,
                concept="CIERRE_DE_CAJA (Sin saldo pendiente)",
                timestamp=datetime.now(timezone.utc)
            )
            session.add(m)
            status = "cierre_sin_saldo"
        
        session.commit()
        return {"status": status, "balance_closed": current_balance}
