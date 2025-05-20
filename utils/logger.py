import logging
import sys


def setup_logger():
    """Настройка логгера."""
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)  # Изменено на DEBUG для отладки

    # Формат сообщений
    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Консольный хэндлер
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Файловый хэндлер
    file_handler = logging.FileHandler("script.log")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)