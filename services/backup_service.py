import gspread
from oauth2client.service_account import ServiceAccountCredentials
from sqlmodel import Session, select, func
from database.models import Product, Sale, Client, Payment
from datetime import datetime
import os

SPREADSHEET_ID = "1oAKLT7SAVn4yfX6Jtm_LXWXS9AiYn8S-UGspV2TBW4w"
CREDENTIALS_FILE = "credentials.json"


def perform_backup(session: Session, tenant_id: int):
    print("INFO: Starting Backup Process...")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

    try:
        import json
        google_creds_env = os.environ.get("GOOGLE_CREDENTIALS")
        if google_creds_env:
            creds_dict = json.loads(google_creds_env)
            creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        else:
            if os.path.exists(CREDENTIALS_FILE):
                creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
            else:
                raise Exception("No credentials found! Set GOOGLE_CREDENTIALS env var or update credentials.json")
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(SPREADSHEET_ID)
    except Exception as e:
        print(f"ERROR authenticating with Google: {e}")
        return {"status": "error", "message": str(e)}

    try:
        try:
            sheet_sales = spreadsheet.worksheet("Ventas")
        except Exception:
            sheet_sales = spreadsheet.add_worksheet(title="Ventas", rows="1000", cols="10")
            sheet_sales.append_row(["Tenant", "ID Venta", "Fecha", "Cliente", "Total", "Pagado", "Metodo", "Items"])

        sales = session.exec(select(Sale).where(Sale.tenant_id == tenant_id).order_by(Sale.timestamp.desc()).limit(100)).all()
        sales_rows = []
        for s in sales:
            client_name = "Mostrador"
            if s.client_id:
                client_obj = session.get(Client, s.client_id)
                if client_obj and client_obj.tenant_id == tenant_id:
                    client_name = client_obj.name
            items_str = ", ".join([f"{i.quantity}x {i.product_name}" for i in s.items])
            sales_rows.append([tenant_id, s.id, s.timestamp.strftime('%Y-%m-%d %H:%M'), client_name, s.total_amount, s.amount_paid, s.payment_method, items_str])
        sheet_sales.clear()
        sheet_sales.append_row(["Tenant", "ID Venta", "Fecha", "Cliente", "Total", "Pagado", "Metodo", "Items"])
        if sales_rows:
            sheet_sales.append_rows(sales_rows)
    except Exception as e:
        print(f"WARNING backing up Sales: {e}")

    try:
        try:
            sheet_debt = spreadsheet.worksheet("Deudores")
        except Exception:
            sheet_debt = spreadsheet.add_worksheet(title="Deudores", rows="1000", cols="6")
            sheet_debt.append_row(["Tenant", "ID Cliente", "Nombre", "Telefono", "Limite Credito", "SALDO DEUDA"])

        clients = session.exec(select(Client).where(Client.tenant_id == tenant_id)).all()
        debtors_rows = []
        total_debt_street = 0
        for c in clients:
            sales_total = session.exec(select(func.sum(Sale.total_amount)).where(Sale.client_id == c.id, Sale.tenant_id == tenant_id)).one() or 0.0
            payments_total = session.exec(select(func.sum(Payment.amount)).where(Payment.client_id == c.id, Payment.tenant_id == tenant_id)).one() or 0.0
            balance = float(sales_total - payments_total)
            if balance > 10:
                debtors_rows.append([tenant_id, c.id, c.name, c.phone, c.credit_limit, balance])
                total_debt_street += balance
        sheet_debt.clear()
        sheet_debt.append_row(["Tenant", "ID Cliente", "Nombre", "Telefono", "Limite Credito", "SALDO DEUDA"])
        if debtors_rows:
            sheet_debt.append_rows(debtors_rows)
        sheet_debt.append_row(["", "", "", "", "TOTAL EN LA CALLE:", total_debt_street])
    except Exception as e:
        print(f"WARNING backing up Debtors: {e}")

    try:
        try:
            sheet_stock = spreadsheet.worksheet("Stock")
        except Exception:
            sheet_stock = spreadsheet.add_worksheet(title="Stock", rows="1000", cols="7")
        products = session.exec(select(Product).where(Product.tenant_id == tenant_id)).all()
        stock_rows = [[tenant_id, p.id, p.item_number, p.name, p.category, p.stock_quantity, p.price] for p in products]
        sheet_stock.clear()
        sheet_stock.append_row(["Tenant", "ID", "Articulo", "Producto", "Categoria", "CANTIDAD", "Precio"])
        if stock_rows:
            sheet_stock.append_rows(stock_rows)
    except Exception as e:
        print(f"WARNING backing up Stock: {e}")

    return {"status": "success", "message": "Backup completed successfully to Google Drive"}


if __name__ == "__main__":
    from database.session import engine

    with Session(engine) as session:
        perform_backup(session, tenant_id=1)
