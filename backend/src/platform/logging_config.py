import logging
import sys


class ContentLengthErrorFilter(logging.Filter):
    """Filter out h11 Content-Length mismatch errors.
    
    The errors don't affect client responses but create
    noisy logs.
    """
    
    def filter(self, record: logging.LogRecord) -> bool:
        # Check the message
        msg = record.getMessage()
        if "Too much data for declared Content-Length" in msg:
            return False
        if "LocalProtocolError" in msg and "Content-Length" in msg:
            return False
        
        # Check exception info (for tracebacks logged with exc_info=True)
        if record.exc_info:
            exc_type, exc_value, _ = record.exc_info
            if exc_value and "Content-Length" in str(exc_value):
                return False
        
        return True


def setup_logging():
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    console_handler.setFormatter(formatter)

    if not root_logger.hasHandlers():
        root_logger.addHandler(console_handler)

    # Filter out noisy h11 Content-Length errors
    content_length_filter = ContentLengthErrorFilter()
    logging.getLogger("uvicorn.error").addFilter(content_length_filter)

    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    logging.getLogger("alembic").setLevel(logging.INFO)
