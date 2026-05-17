import os
from cryptography.fernet import Fernet
from sqlmodel import Session, create_engine, select

def main():
    env_path = ".env"
    fernet_key = None

    # 1. Leer la clave Fernet si existe
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            content = f.read()
        for line in content.split("\n"):
            if line.startswith("NEXPOS_FERNET_KEY="):
                fernet_key = line.split("=", 1)[1].strip()

    # 2. Generar la clave si no existe y agregarla al .env
    if not fernet_key:
        fernet_key = Fernet.generate_key().decode()
        with open(env_path, "a") as f:
            f.write(f"\n# Clave para cifrado de credenciales (generada para Fix 7)\n")
            f.write(f"NEXPOS_FERNET_KEY={fernet_key}\n")
        print("✅ Generada nueva clave NEXPOS_FERNET_KEY en el archivo .env")
    else:
        print("ℹ️ Clave NEXPOS_FERNET_KEY ya existe en .env")

    # Guardar en el entorno para que database/models.py pueda usarla
    os.environ["NEXPOS_FERNET_KEY"] = fernet_key

    # Importar despues de setear la variable de entorno
    from database.models import AICredential, BusinessConfig, encrypt_api_key

    DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///./production.db")
    engine = create_engine(DATABASE_URL)

    # 3. Leer las claves que se renombraron y cifrarlas
    with Session(engine) as session:
        credentials = session.exec(select(AICredential)).all()
        count_ai = 0
        for cred in credentials:
            raw = cred.api_key_enc
            # Si hay algo y NO está cifrado con Fernet (los tokens de Fernet empiezan con gAAAA)
            if raw and not raw.startswith("gAAAA"):
                cred.api_key_enc = encrypt_api_key(raw)
                session.add(cred)
                count_ai += 1
                
        configs = session.exec(select(BusinessConfig)).all()
        count_conf = 0
        for cfg in configs:
            for field in ("openai_api_key_enc", "deepseek_api_key_enc", "elevenlabs_api_key_enc"):
                val = getattr(cfg, field, None)
                if val and not val.startswith("gAAAA"):
                    setattr(cfg, field, encrypt_api_key(val))
                    session.add(cfg)
                    count_conf += 1
                    
        session.commit()
        print(f"✅ Se han cifrado y reparado {count_ai} claves en AICredential")
        print(f"✅ Se han cifrado y reparado {count_conf} claves en BusinessConfig")
        print("¡Todo listo! Las credenciales han sido restauradas correctamente.")

if __name__ == "__main__":
    main()
