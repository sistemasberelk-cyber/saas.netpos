from passlib.context import CryptContext
from sqlmodel import Session, select
from database.models import User, Settings, Tenant
import os
import secrets

pwd_context = CryptContext(schemes=["argon2", "bcrypt"], deprecated="auto")
print(f"INFO: Password Context Schemes: {pwd_context.schemes()}")

def _get_secure_password(env_var: str, label: str) -> str:
    """Get password from env var. If not set, generate a secure random one and warn."""
    password = os.getenv(env_var)
    if password:
        return password
    generated = secrets.token_urlsafe(16)
    print(f"\n{'='*60}")
    print(f"⚠️  SEGURIDAD: No se encontró {env_var} en variables de entorno.")
    print(f"   Se generó una contraseña segura para '{label}':")
    print(f"   👤 {label}: {generated}")
    print(f"   ⚠️  GUÁRDALA Y CONFIGÚRALA EN TUS VARIABLES DE ENTORNO")
    print(f"{'='*60}\n")
    return generated


class AuthService:
    @staticmethod
    def verify_password(plain_password, hashed_password):
        return pwd_context.verify(plain_password, hashed_password)

    @staticmethod
    def get_password_hash(password):
        return pwd_context.hash(password)

    @staticmethod
    def create_default_user_and_settings(session: Session):
        tenant = session.exec(select(Tenant).order_by(Tenant.id)).first()
        if not tenant:
            tenant = Tenant(name="Default Company", subdomain="default")
            session.add(tenant)
            session.commit()
            session.refresh(tenant)
            print(f"INFO: Created default Tenant (ID: {tenant.id})")

        # 1. Create or sync admin
        user = session.exec(select(User).where(User.username == "admin", User.tenant_id == tenant.id)).first()
        if not user:
            default_password = _get_secure_password("ADMIN_PASSWORD", "admin")
            hashed = AuthService.get_password_hash(default_password)
            user = User(
                username="admin",
                password_hash=hashed,
                role="admin",
                full_name="Administrador",
                tenant_id=tenant.id,
            )
            session.add(user)
            print(f"INFO: Created default user 'admin' (Tenant: {tenant.id})")
        else:
            # Sync admin password from env var if set
            admin_env_pw = os.getenv("ADMIN_PASSWORD")
            if admin_env_pw and not AuthService.verify_password(admin_env_pw, user.password_hash):
                user.password_hash = AuthService.get_password_hash(admin_env_pw)
                session.add(user)
                print("INFO: Admin password synced from ADMIN_PASSWORD env var")

        # 2. Create or sync superadmin
        superadmin = session.exec(select(User).where(User.username == "superadmin")).first()
        superadmin_env_pw = os.getenv("SUPERADMIN_PASSWORD")
        if not superadmin:
            default_password = _get_secure_password("SUPERADMIN_PASSWORD", "superadmin")
            hashed = AuthService.get_password_hash(default_password)
            superadmin = User(
                username="superadmin",
                password_hash=hashed,
                role="superadmin",
                full_name="Super Administrador Global",
                tenant_id=tenant.id,
            )
            session.add(superadmin)
            print(f"INFO: Created default user 'superadmin'")
        elif superadmin_env_pw:
            if not AuthService.verify_password(superadmin_env_pw, superadmin.password_hash):
                superadmin.password_hash = AuthService.get_password_hash(superadmin_env_pw)
                session.add(superadmin)
                print("INFO: Superadmin password synced from SUPERADMIN_PASSWORD env var")

        # 3. Create default settings
        settings = session.exec(select(Settings).where(Settings.tenant_id == tenant.id)).first()
        if not settings:
            default_settings = Settings(
                tenant_id=tenant.id,
                company_name="NexPos",
                logo_url="/static/images/logo.png",
            )
            session.add(default_settings)
            print("INFO: Created default settings for Tenant")

        session.commit()
