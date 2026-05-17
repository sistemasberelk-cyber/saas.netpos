import sqlite3
import os
from datetime import datetime, timezone
from cryptography.fernet import Fernet

def main():
    print("Iniciando rescate de base de datos...")
    # 1. Obtener la clave Fernet
    env_path = ".env"
    fernet_key = None
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            for line in f.read().split("\n"):
                if line.startswith("NEXPOS_FERNET_KEY="):
                    fernet_key = line.split("=", 1)[1].strip()
    
    if not fernet_key:
        fernet_key = Fernet.generate_key().decode()
        with open(env_path, "a") as f:
            f.write(f"\nNEXPOS_FERNET_KEY={fernet_key}\n")
        print("✅ Generada nueva clave NEXPOS_FERNET_KEY en .env")
    
    f = Fernet(fernet_key.encode())
    
    db_path = "production.db"
    if not os.path.exists(db_path):
        print("❌ No se encontró production.db")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # --- FIX STOCK (Backfill) ---
    print("📦 Migrando stock legado a nuevo WMS...")
    cursor.execute("SELECT id, name FROM tenant WHERE is_active = 1")
    tenants = cursor.fetchall()
    
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
    
    for tenant_id, tenant_name in tenants:
        # Location
        cursor.execute("SELECT id FROM location WHERE tenant_id = ? AND code = 'GENERAL'", (tenant_id,))
        loc = cursor.fetchone()
        if not loc:
            cursor.execute("INSERT INTO location (tenant_id, name, code, description, is_active, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                           (tenant_id, "Stock General", "GENERAL", "Migracion", 1, now))
            loc_id = cursor.lastrowid
        else:
            loc_id = loc[0]
            
        # Bin
        cursor.execute("SELECT id FROM bin WHERE tenant_id = ? AND location_id = ? AND name = 'STOCK'", (tenant_id, loc_id))
        bn = cursor.fetchone()
        if not bn:
            cursor.execute("INSERT INTO bin (tenant_id, location_id, name, description, is_active) VALUES (?, ?, ?, ?, ?)",
                           (tenant_id, loc_id, "STOCK", "Migracion", 1))
            bin_id = cursor.lastrowid
        else:
            bin_id = bn[0]
            
        # Move stock
        try:
            cursor.execute("SELECT id, stock_quantity FROM product WHERE tenant_id = ? AND is_deleted = 0", (tenant_id,))
            products = cursor.fetchall()
            migrados = 0
            for prod_id, qty in products:
                if qty is None: qty = 0
                if qty < 0: qty = 0
                
                cursor.execute("SELECT id FROM binstock WHERE tenant_id = ? AND product_id = ?", (tenant_id, prod_id))
                if cursor.fetchone():
                    continue
                    
                cursor.execute("INSERT INTO binstock (tenant_id, bin_id, product_id, quantity, updated_at) VALUES (?, ?, ?, ?, ?)",
                               (tenant_id, bin_id, prod_id, qty, now))
                               
                if qty > 0:
                    cursor.execute("INSERT INTO stockmovement (tenant_id, product_id, to_bin_id, quantity, reason, notes, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)",
                                   (tenant_id, prod_id, bin_id, qty, "backfill_migracion", "Migracion autom.", now))
                migrados += 1
            if migrados > 0:
                print(f"  ✅ Tenant {tenant_id}: {migrados} productos migrados al WMS.")
        except sqlite3.OperationalError:
            print("  ⚠️ stock_quantity ya no existe (la migración ya se aplicó).")
                               
    # --- ENCRIPTAR CREDENCIALES ---
    print("🔐 Cifrando credenciales API...")
    cursor.execute("PRAGMA table_info(aicredential)")
    cols = [c[1] for c in cursor.fetchall()]
    col_name = "api_key_enc" if "api_key_enc" in cols else "api_key"
    
    if col_name in cols:
        cursor.execute(f"SELECT id, {col_name} FROM aicredential")
        count_ai = 0
        for row_id, val in cursor.fetchall():
            if val and not val.startswith("gAAAA"):
                enc = f.encrypt(val.encode()).decode()
                cursor.execute(f"UPDATE aicredential SET {col_name} = ? WHERE id = ?", (enc, row_id))
                count_ai += 1
        if count_ai > 0:
            print(f"  ✅ {count_ai} claves cifradas en AICredential.")
            
    cursor.execute("PRAGMA table_info(businessconfig)")
    cols = [c[1] for c in cursor.fetchall()]
    
    fields = [("openai_api_key", "openai_api_key_enc"), 
              ("deepseek_api_key", "deepseek_api_key_enc"), 
              ("elevenlabs_api_key", "elevenlabs_api_key_enc")]
              
    count_conf = 0
    for old_col, new_col in fields:
        col = new_col if new_col in cols else old_col
        if col in cols:
            cursor.execute(f"SELECT id, {col} FROM businessconfig")
            for row_id, val in cursor.fetchall():
                if val and not val.startswith("gAAAA"):
                    enc = f.encrypt(val.encode()).decode()
                    cursor.execute(f"UPDATE businessconfig SET {col} = ? WHERE id = ?", (enc, row_id))
                    count_conf += 1
    if count_conf > 0:
        print(f"  ✅ {count_conf} claves cifradas en BusinessConfig.")
                    
    conn.commit()
    conn.close()
    print("=======================================================")
    print("🚀 TODO LISTO! Ahora puedes ejecutar 'alembic upgrade head'")
    print("=======================================================")

if __name__ == "__main__":
    main()
