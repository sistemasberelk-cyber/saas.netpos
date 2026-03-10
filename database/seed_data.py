from sqlmodel import Session, select
from database.models import Product, Tenant


def seed_products(session: Session):
    tenant = session.exec(select(Tenant).order_by(Tenant.id)).first()
    if not tenant:
        return

    products_data = [
        {
            "item_number": "7111",
            "name": "Gomon Pin Negro",
            "description": "Gomon Pin Negro - 35 al 40 - 12 Pares",
            "price": 7500.0,
            "numeracion": "35 al 40",
            "cant_bulto": 12,
            "stock_quantity": 120,
            "barcode": "711100000001",
            "category": "Calzado"
        },
        {
            "item_number": "7098",
            "name": "Gomon NO Pin",
            "description": "Gomon NO Pin - 35 al 40 - 12 Pares",
            "price": 6000.0,
            "numeracion": "35 al 40",
            "cant_bulto": 12,
            "stock_quantity": 120,
            "barcode": "709800000001",
            "category": "Calzado"
        },
        {
            "item_number": "7110",
            "name": "Articulo 7110",
            "description": "Art 7110 - 35 al 40 - 12 Pares",
            "price": 13000.0,
            "numeracion": "35 al 40",
            "cant_bulto": 12,
            "stock_quantity": 120,
            "barcode": "711000000001",
            "category": "Calzado"
        },
        {
            "item_number": "7083",
            "name": "Gomon 1/2 Alto",
            "description": "1/2 Alto - 35 al 40 - 12 Surtido",
            "price": 8500.0,
            "numeracion": "35 al 40",
            "cant_bulto": 12,
            "stock_quantity": 120,
            "barcode": "708300000001",
            "category": "Calzado"
        },
        {
            "item_number": "7091",
            "name": "Articulo 7091",
            "description": "Art 7091 - 35/6 al 39/0 - 12 Pares Surtidos",
            "price": 7200.0,
            "numeracion": "35/6 al 39/0",
            "cant_bulto": 12,
            "stock_quantity": 120,
            "barcode": "709100000001",
        },
        {
            "item_number": "7108",
            "name": "Articulo 7108",
            "description": "Art 7108 - 35 al 40 - 12 Ps x Color",
            "price": 12000.0,
            "numeracion": "35 al 40",
            "cant_bulto": 12,
            "stock_quantity": 120,
            "barcode": "710800000001",
            "category": "Calzado"
        },
        {
            "item_number": "7152",
            "name": "Articulo 7152",
            "description": "Art 7152 - 24 al 29 - 12 Ps Surtido",
            "price": 7500.0,
            "numeracion": "24 al 29",
            "cant_bulto": 12,
            "stock_quantity": 120,
            "barcode": "715200000001",
            "category": "Calzado"
        },
        {
            "item_number": "7183",
            "name": "Articulo 7183",
            "description": "Art 7183 - 35/6 al 39/0 - 12 S",
            "price": 10000.0,
            "numeracion": "35/6 al 39/0",
            "cant_bulto": 12,
            "stock_quantity": 120,
            "barcode": "718300000001",
            "category": "Calzado"
        },
        {
            "item_number": "158",
            "name": "Articulo 158",
            "description": "Art 158 - 24 al 30 - 12 Ps Surt",
            "price": 5450.0,
            "numeracion": "24 al 30",
            "cant_bulto": 12,
            "stock_quantity": 120,
            "barcode": "015800000001",
            "category": "Calzado"
        },
        {
            "item_number": "7102",
            "name": "Articulo 7102",
            "description": "Art 7102 - 35/6 al 39/0 - 12 x C",
            "price": 6500.0,
            "numeracion": "35/6 al 39/0",
            "cant_bulto": 12,
            "stock_quantity": 120,
            "barcode": "710200000001",
            "category": "Calzado"
        }
    ]

    for p_data in products_data:
        statement = select(Product).where(Product.item_number == p_data["item_number"], Product.tenant_id == tenant.id)
        product = session.exec(statement).first()
        if not product:
            session.add(Product(tenant_id=tenant.id, **p_data))
            print(f"Adding product: {p_data['name']}")

    session.commit()
