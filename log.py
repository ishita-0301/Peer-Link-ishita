import logging

logging.basicConfig(
    filename="OUTPUT.txt",
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

_logger = logging.getLogger(__name__)


def log(message: str):
    _logger.info(message)
