from sqlmodel import Session, select
from database.session import engine, create_db_and_tables
from database.models import User
from services.auth_service import AuthService

def fix_admin():
    # Ensure tables exist (just in case)
    # create_db_and_tables() # skipped to avoid recreating if it exists
    
    with Session(engine) as session:
        print("Searching for admin user...")
        user = session.exec(select(User).where(User.username == "admin")).first()

        new_hash = AuthService.get_password_hash("admin123")

        if user:
            print(f"Found existing admin user (ID: {user.id}). Updating password...")
            user.password_hash = new_hash
            user.role = "admin" # Ensure role is admin
            session.add(user)
        else:
            print("Admin user not found. Creating new one...")
            user = User(username="admin", password_hash=new_hash, role="admin", full_name="System Administrator")
            session.add(user)

        session.commit()
    print("SUCCESS: Admin password reset to 'admin123'")

if __name__ == "__main__":
    fix_admin()
