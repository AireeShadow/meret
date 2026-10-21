import logging

__version__ = "0.6.15"

# silent as a library; entry points configure logging via meret.core.logger.setup_logging
logging.getLogger(__name__).addHandler(logging.NullHandler())
