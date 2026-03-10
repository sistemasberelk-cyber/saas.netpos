from sqlmodel import Session

from database.session import engine
from services.migration_service import run_schema_migrations


def main():
    with Session(engine) as session:
        results = run_schema_migrations(session)
        for line in results:
            print(line)


if __name__ == "__main__":
    main()
