import logging
import sys
import uuid

class TraceIdFilter(logging.Filter):
    def filter(self, record):
        # Attempt to get trace_id from the current request context (if set by middleware)
        # or from the log record itself if explicitly added.
        # If not found, default to an empty string or 'N/A'.
        record.trace_id = getattr(record, 'trace_id', 'N/A')
        return True

def setup_logging():
    # Create a custom formatter to include trace_id
    # asctime is already included by default in basicConfig format if not specified
    # We'll explicitly define it to ensure consistency and add trace_id
    formatter = logging.Formatter(
        "[%(trace_id)s] - %(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Get the root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Clear existing handlers to prevent duplicate logs if setup_logging is called multiple times
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    # Add a stream handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)

    # Add the TraceIdFilter to the handler
    handler.addFilter(TraceIdFilter())

    # Optionally, set a higher level for specific noisy loggers
    logging.getLogger("uvicorn").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
