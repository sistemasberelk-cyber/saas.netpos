"""
scripts/backfill_stock_to_wms.py
=================================
EJECUTAR ANTES de la migración vibecloud_all_fixes (FIX #4).

Crea un Location "Stock General" y un Bin "STOCK" por tenant,
y copia product.stock_quantity → BinStock.quantity para todos los
productos que aún no tienen stock en BinStock.

Uso:
    python scripts/backfill_stock_to_wms.py

Requiere DATABASE_URL en el entorno.
"""
import os
from datetime import datetime, timezone
from sqlmodel import Session, create_engine, select

# Importar modelos ANTES de correr la migración (con stock_quantity aún presente)
from database.models import (
    BinStock,
    Bin,
    Location,
    Product,
    StockMovement,
    Tenant,
)

DATABASE_URL = os.environ["DATABASE_URL"]
engine = create_engine(DATABASE_URL)


def _utcnow():
    return datetime.now(timezone.utc)


def backfill(session: Session) -> None:
    tenants = session.exec(select(Tenant).where(Tenant.is_active == True)).all()
    print(f"Tenants encontrados: {len(tenants)}")

    for tenant in tenants:
        print(f"\n→ Tenant: {tenant.name} (id={tenant.id})")

        # 1. Crear depósito "Stock General" si no existe
        location = session.exec(
            select(Location).where(
                Location.tenant_id == tenant.id,
                Location.code == "GENERAL",
            )
        ).first()

        if not location:
            location = Location(
                tenant_id=tenant.id,
                name="Stock General",
                code="GENERAL",
                description="Depósito creado automáticamente por backfill de migración",
                is_active=True,
                created_at=_utcnow(),
            )
            session.add(location)
            session.flush()
            print(f"  Creado Location id={location.id}")
        else:
            print(f"  Location GENERAL ya existe id={location.id}")

        # 2. Crear bin "STOCK" si no existe
        bin_ = session.exec(
            select(Bin).where(
                Bin.tenant_id == tenant.id,
                Bin.location_id == location.id,
                Bin.name == "STOCK",
            )
        ).first()

        if not bin_:
            bin_ = Bin(
                tenant_id=tenant.id,
                location_id=location.id,
                name="STOCK",
                description="Bin por defecto para migración de stock legacy",
                is_active=True,
            )
            session.add(bin_)
            session.flush()
            print(f"  Creado Bin id={bin_.id}")
        else:
            print(f"  Bin STOCK ya existe id={bin_.id}")

        # 3. Migrar stock de cada producto
        products = session.exec(
            select(Product).where(
                Product.tenant_id == tenant.id,
                Product.is_deleted == False,
            )
        ).all()

        migrated = 0
        skipped = 0

        for product in products:
            # ¿Ya tiene BinStock en algún bin?
            existing = session.exec(
                select(BinStock).where(
                    BinStock.tenant_id == tenant.id,
                    BinStock.product_id == product.id,
                )
            ).first()

            if existing:
                skipped += 1
                continue

            qty = getattr(product, "stock_quantity", 0) or 0
            if qty < 0:
                qty = 0

            bin_stock = BinStock(
                tenant_id=tenant.id,
                bin_id=bin_.id,
                product_id=product.id,
                quantity=qty,
                updated_at=_utcnow(),
            )
            session.add(bin_stock)

            if qty > 0:
                movement = StockMovement(
                    tenant_id=tenant.id,
                    product_id=product.id,
                    from_bin_id=None,
                    to_bin_id=bin_.id,
                    quantity=qty,
                    reason="backfill_migracion",
                    notes=f"Migración automática desde stock_quantity={qty}",
                )
                session.add(movement)

            migrated += 1

        session.commit()
        print(f"  Productos migrados: {migrated} | Ya tenían BinStock: {skipped}")

    print("\n✅ Backfill completado. Podés correr la migración vibecloud_all_fixes.")


def encrypt_existing_api_keys(session: Session) -> None:
    """
    Cifra las api_key existentes con Fernet antes de renombrar la columna.
    Ejecutar también antes de vibecloud_all_fixes (FIX #7).
    """
    from database.models import AICredential, BusinessConfig
    from database.models import encrypt_api_key

    credentials = session.exec(select(AICredential)).all()
    for cred in credentials:
        raw = cred.api_key  # columna vieja, aún existe
        if raw and not raw.startswith("gAAAA"):  # Fernet tokens empiezan con gAAAA
            cred.api_key = encrypt_api_key(raw)
            session.add(cred)
    session.commit()
    print(f"✅ {len(credentials)} AICredential(s) cifradas.")

    configs = session.exec(select(BusinessConfig)).all()
    for cfg in configs:
        for field in ("openai_api_key", "deepseek_api_key", "elevenlabs_api_key"):
            val = getattr(cfg, field, None)
            if val and not val.startswith("gAAAA"):
                setattr(cfg, field, encrypt_api_key(val))
                session.add(cfg)
    session.commit()
    print(f"✅ {len(configs)} BusinessConfig(s) cifradas.")


if __name__ == "__main__":
    import sys

    with Session(engine) as session:
        if "--encrypt-keys" in sys.argv:
            encrypt_existing_api_keys(session)
        else:
            backfill(session)
