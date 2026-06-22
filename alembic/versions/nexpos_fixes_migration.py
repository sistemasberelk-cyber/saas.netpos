"""vibecloud_all_fixes

Aplica TODAS las correcciones al schema de VibeCloud SaaS:
  1. ui_theme server_default corregido a 'standard'
  2. Soft delete (is_deleted + deleted_at) en Supplier, User, Purchase, Location, Bin
  3. barcode: UniqueConstraint(tenant_id, barcode) — elimina unique global
  4. stock_quantity eliminado de product
  5. Tabla accountreceivable (cuentas corrientes reales)
  6. Tabla paymentallocation (N métodos de pago por venta)
  7. AICredential: api_key → api_key_enc (texto plano eliminado)
  8. CashMovement: sale_id + purchase_id restaurados como FK tipadas

Revision ID: vibecloud_all_fixes
Revises: 47e6409a3538
Create Date: 2026-05-17
"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

revision: str = "vibecloud_all_fixes"
down_revision: Union[str, Sequence[str], None] = "47e6409a3538"
branch_labels = None
depends_on = None


def upgrade() -> None:

    # -----------------------------------------------------------------------
    # FIX 1: ui_theme — alinear server_default con el modelo
    # (ya existe la columna, solo actualizamos filas con valor incorrecto)
    # -----------------------------------------------------------------------
    op.execute(
        "UPDATE settings SET ui_theme = 'standard' WHERE ui_theme = 'default'"
    )

    # -----------------------------------------------------------------------
    # FIX 2: Soft delete — agregar is_deleted + deleted_at donde falta
    # -----------------------------------------------------------------------
    for table in ("supplier", "user", "purchase", "location", "bin"):
        with op.batch_alter_table(table) as batch_op:
            batch_op.add_column(
                sa.Column("is_deleted", sa.Boolean(), nullable=False, server_default=sa.false())
            )
            batch_op.add_column(
                sa.Column("deleted_at", sa.DateTime(), nullable=True)
            )

    # -----------------------------------------------------------------------
    # FIX 3: barcode — eliminar unique global, crear unique por tenant
    # -----------------------------------------------------------------------
    with op.batch_alter_table("product") as batch_op:
        # Crear unique compuesto por tenant
        batch_op.create_unique_constraint(
            "uq_product_tenant_barcode", ["tenant_id", "barcode"]
        )

    # -----------------------------------------------------------------------
    # FIX 4: Eliminar stock_quantity de product (fuente única: BinStock)
    # PRECAUCIÓN: hacer backfill a BinStock ANTES de correr este paso
    # en producción. Ver scripts/backfill_stock_to_wms.py
    # -----------------------------------------------------------------------
    with op.batch_alter_table("product") as batch_op:
        batch_op.drop_column("stock_quantity")

    # -----------------------------------------------------------------------
    # FIX 5: AccountReceivable — cuentas corrientes reales
    # -----------------------------------------------------------------------
    op.create_table(
        "accountreceivable",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("tenant_id", sa.Integer(), sa.ForeignKey("tenant.id"), nullable=True),
        sa.Column("sale_id", sa.Integer(), sa.ForeignKey("sale.id"), nullable=False, unique=True),
        sa.Column("client_id", sa.Integer(), sa.ForeignKey("client.id"), nullable=False),
        sa.Column("invoice_number", sa.String(), nullable=True, index=True),
        sa.Column("total", sa.Float(), nullable=False),
        sa.Column("paid", sa.Float(), nullable=False, server_default="0"),
        sa.Column("balance", sa.Float(), nullable=False),
        sa.Column("issued_at", sa.DateTime(), nullable=False),
        sa.Column("due_date", sa.DateTime(), nullable=True),
        sa.Column("status", sa.String(), nullable=False, server_default="pending"),
    )
    op.create_index("ix_accountreceivable_client", "accountreceivable", ["client_id"])

    # -----------------------------------------------------------------------
    # FIX 5b: agregar receivable_id a payment para relacionar deuda específica
    # -----------------------------------------------------------------------
    with op.batch_alter_table("payment") as batch_op:
        batch_op.add_column(
            sa.Column("receivable_id", sa.Integer(), sa.ForeignKey("accountreceivable.id"), nullable=True)
        )
        batch_op.add_column(
            sa.Column("method", sa.String(), nullable=False, server_default="cash")
        )

    # -----------------------------------------------------------------------
    # FIX 6: PaymentAllocation — N métodos de pago por venta
    # -----------------------------------------------------------------------
    op.create_table(
        "paymentallocation",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("sale_id", sa.Integer(), sa.ForeignKey("sale.id"), nullable=False, index=True),
        sa.Column("method", sa.String(), nullable=False),
        sa.Column("amount", sa.Float(), nullable=False),
    )

    # Migrar datos existentes de amount_cash / amount_transfer a paymentallocation
    op.execute("""
        INSERT INTO paymentallocation (sale_id, method, amount)
        SELECT id, 'cash', amount_cash
        FROM sale
        WHERE amount_cash > 0
    """)
    op.execute("""
        INSERT INTO paymentallocation (sale_id, method, amount)
        SELECT id, 'transfer', amount_transfer
        FROM sale
        WHERE amount_transfer > 0
    """)

    # Eliminar columnas legacy de sale
    with op.batch_alter_table("sale") as batch_op:
        batch_op.drop_column("amount_cash")
        batch_op.drop_column("amount_transfer")
        batch_op.drop_column("payment_method")  # reemplazado por paymentallocation

    # -----------------------------------------------------------------------
    # FIX 7: AICredential — renombrar api_key → api_key_enc
    # PRECAUCIÓN: cifrar todos los valores existentes ANTES con:
    #   python scripts/encrypt_existing_api_keys.py
    # -----------------------------------------------------------------------
    with op.batch_alter_table("aicredential") as batch_op:
        batch_op.alter_column("api_key", new_column_name="api_key_enc")

    with op.batch_alter_table("businessconfig") as batch_op:
        batch_op.alter_column("openai_api_key", new_column_name="openai_api_key_enc")
        batch_op.alter_column("deepseek_api_key", new_column_name="deepseek_api_key_enc")
        batch_op.alter_column("elevenlabs_api_key", new_column_name="elevenlabs_api_key_enc")

    # -----------------------------------------------------------------------
    # FIX 8: CashMovement — restaurar sale_id y purchase_id con FK tipadas
    # -----------------------------------------------------------------------
    with op.batch_alter_table("cashmovement") as batch_op:
        batch_op.add_column(
            sa.Column("sale_id", sa.Integer(), sa.ForeignKey("sale.id"), nullable=True, index=True)
        )
        batch_op.add_column(
            sa.Column("purchase_id", sa.Integer(), sa.ForeignKey("purchase.id"), nullable=True, index=True)
        )
        # Backfill: si reference_type='sale', copiar reference_id → sale_id
    op.execute("""
        UPDATE cashmovement
        SET sale_id = reference_id
        WHERE reference_type = 'sale' AND reference_id IS NOT NULL
    """)
    op.execute("""
        UPDATE cashmovement
        SET purchase_id = reference_id
        WHERE reference_type = 'purchase' AND reference_id IS NOT NULL
    """)


def downgrade() -> None:
    # FIX 8
    with op.batch_alter_table("cashmovement") as batch_op:
        batch_op.drop_column("purchase_id")
        batch_op.drop_column("sale_id")

    # FIX 7
    with op.batch_alter_table("aicredential") as batch_op:
        batch_op.alter_column("api_key_enc", new_column_name="api_key")
    with op.batch_alter_table("businessconfig") as batch_op:
        batch_op.alter_column("openai_api_key_enc", new_column_name="openai_api_key")
        batch_op.alter_column("deepseek_api_key_enc", new_column_name="deepseek_api_key")
        batch_op.alter_column("elevenlabs_api_key_enc", new_column_name="elevenlabs_api_key")

    # FIX 6
    op.drop_table("paymentallocation")
    with op.batch_alter_table("sale") as batch_op:
        batch_op.add_column(sa.Column("payment_method", sa.String(), nullable=False, server_default="cash"))
        batch_op.add_column(sa.Column("amount_cash", sa.Float(), nullable=False, server_default="0"))
        batch_op.add_column(sa.Column("amount_transfer", sa.Float(), nullable=False, server_default="0"))

    # FIX 5
    with op.batch_alter_table("payment") as batch_op:
        batch_op.drop_column("receivable_id")
        batch_op.drop_column("method")
    op.drop_index("ix_accountreceivable_client", "accountreceivable")
    op.drop_table("accountreceivable")

    # FIX 4
    with op.batch_alter_table("product") as batch_op:
        batch_op.add_column(sa.Column("stock_quantity", sa.Integer(), nullable=False, server_default="0"))

    # FIX 3
    with op.batch_alter_table("product") as batch_op:
        batch_op.drop_constraint("uq_product_tenant_barcode", type_="unique")

    # FIX 2
    for table in ("supplier", "user", "purchase", "location", "bin"):
        with op.batch_alter_table(table) as batch_op:
            batch_op.drop_column("deleted_at")
            batch_op.drop_column("is_deleted")
