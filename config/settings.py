from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    mysql_host: str = "localhost"
    mysql_user: str = "root"
    mysql_password: str = ""
    mysql_database: str = "futures_db"
    collection_interval: int = 60  # Интервал сбора данных в секундах

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "allow"