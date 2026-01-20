from os import environ
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from contextlib import contextmanager

DATABASE_URL = environ["DATABASE_URL"]

# Configure SQL echo from environment (defaults to False for production)
SQL_ECHO = environ.get("SQL_ECHO", "").lower() in ("true", "1", "yes")

engine = create_engine(DATABASE_URL, echo=SQL_ECHO)
Session = sessionmaker(bind=engine)

# Scoped session for thread-safe session management
ScopedSession = scoped_session(Session)


@contextmanager
def get_session():
    """
    Context manager for database sessions.
    
    Usage:
        with get_session() as session:
            # use session
            session.commit()
    
    Automatically rolls back on exception and closes session.
    """
    session = Session()
    try:
        yield session
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
