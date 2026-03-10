from passlib.context import CryptContext
from sqlmodel import Session, select
from database.models import User, Settings, Tenant
import os
import secrets

pwd_context = CryptContext(schemes=["argon2", "bcrypt"], deprecated="auto")
print(f"INFO: Password Context Schemes: {pwd_context.schemes()}")


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

        user = session.exec(select(User).where(User.username == "admin", User.tenant_id == tenant.id)).first()
        if not user:
            default_password = os.getenv("ADMIN_PASSWORD")
            if not default_password:
                default_password = secrets.token_urlsafe(12)
                print(f"WARNING: ADMIN_PASSWORD not set. Generated temporary admin password: {default_password}")
            hashed = AuthService.get_password_hash(default_password)
            admin = User(
                username="admin",
                password_hash=hashed,
                role="admin",
                full_name="Administrador",
                tenant_id=tenant.id,
            )
            session.add(admin)
            print(f"INFO: Created default user 'admin' (Tenant: {tenant.id})")

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
