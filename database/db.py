import aiomysql
import logging
from typing import List, Dict
from config.settings import Settings
from datetime import datetime


class Database:
    """Класс для работы с MySQL."""

    def __init__(self, settings: Settings):
        """Инициализация подключения."""
        self.settings = settings
        self.pool = None
        self.logger = logging.getLogger(__name__)

    async def connect(self):
        """Создание пула соединений."""
        self.pool = await aiomysql.create_pool(
            host=self.settings.mysql_host,
            user=self.settings.mysql_user,
            password=self.settings.mysql_password,
            db=self.settings.mysql_database,
            autocommit=True
        )
        self.logger.info(f"Подключение к базе данных: {self.settings.mysql_host}/{self.settings.mysql_database}")

    async def close(self):
        """Закрытие пула соединений."""
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
            self.logger.info("Пул соединений закрыт")

    async def save_tokens(self, symbols: List[str]):
        """Сохранение уникальных токенов."""
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = "SELECT symbol FROM tokens WHERE symbol IN %s"
                await cur.execute(query, (tuple(symbols),))
                existing_symbols = {row[0] for row in await cur.fetchall()}

                new_symbols = [s for s in symbols if s not in existing_symbols]

                if new_symbols:
                    query = """
                            INSERT INTO tokens (symbol)
                            VALUES (%s) \
                            """
                    await cur.executemany(query, [(s,) for s in new_symbols])
                    self.logger.info(f"Сохранено {cur.rowcount} новых токенов")
                else:
                    self.logger.info("Нет новых токенов для сохранения")

    async def save_futures_pairs(self, exchange: str, pairs: List[Dict]):
        """Сохранение фьючерсных пар."""
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                if not pairs:
                    self.logger.warning(f"Пустой список пар для биржи {exchange}. Пропускаем сохранение.")
                    return

                query = "SELECT id, symbol FROM tokens WHERE symbol IN %s"
                base_symbols = tuple(p["base_symbol"] for p in pairs)
                await cur.execute(query, (base_symbols,))
                token_map = {row[1]: row[0] for row in await cur.fetchall()}
                if not token_map:
                    self.logger.warning(f"Не найдены токены для биржи {exchange}: {base_symbols[:5]}")
                    return

                pair_symbols = [
                    p["symbol"].replace("_", "-").upper()
                    for p in pairs if p["base_symbol"] in token_map
                ]
                if not pair_symbols:
                    self.logger.warning(
                        f"Нет подходящих пар для биржи {exchange} после фильтрации токенов."
                    )
                    return

                query = """
                        SELECT pair_symbol
                        FROM futures_pairs
                        WHERE exchange = %s \
                          AND pair_symbol IN %s \
                        """
                await cur.execute(query, (exchange, tuple(pair_symbols)))
                existing_pairs = {row[0] for row in await cur.fetchall()}

                new_pairs = [
                    (token_map[p["base_symbol"]], exchange, p["symbol"].replace("_", "-").upper())
                    for p in pairs
                    if p["base_symbol"] in token_map and p["symbol"].replace("_", "-").upper() not in existing_pairs
                ]

                if new_pairs:
                    query = """
                            INSERT \
                            IGNORE INTO futures_pairs (token_id, exchange, pair_symbol)
                        VALUES ( \
                            %s, \
                            %s, \
                            %s \
                            ) \
                            """
                    await cur.executemany(query, new_pairs)
                    self.logger.info(f"Сохранено {cur.rowcount} новых пар для {exchange}")
                else:
                    self.logger.info(
                        f"Нет новых пар для сохранения на бирже {exchange}."
                    )

    async def save_futures_data(self, data: List[Dict]):
        """Сохранение данных о парах."""
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = """
                        SELECT id, pair_symbol, exchange
                        FROM futures_pairs
                        WHERE (pair_symbol, exchange) IN %s \
                        """
                pairs = [(d["pair_symbol"].replace("_", "-").upper(), d["exchange"]) for d in data]
                await cur.execute(query, (tuple(pairs),))
                pair_map = {f"{row[1]}_{row[2]}": row[0] for row in await cur.fetchall()}
                if not pair_map:
                    self.logger.warning("Таблица futures_pairs пуста или не содержит подходящих пар")

                query = """
                        INSERT INTO futures_data (pair_id, timestamp, open_interest_contracts, open_interest_usd, \
                                                  funding_rate, volume_btc, volume_usd, price_usd, market_cap_usd)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) \
                        """
                values = []
                for d in data:
                    key = f"{d['pair_symbol'].replace('_', '-').upper()}_{d['exchange']}"
                    if key not in pair_map:
                        self.logger.warning(f"Пара {d['pair_symbol']} для {d['exchange']} не найдена в futures_pairs")
                        continue
                    timestamp = d.get("timestamp")
                    if timestamp is None:
                        self.logger.warning(
                            f"timestamp is None для пары {d['pair_symbol']} ({d['exchange']}), пропускаем")
                        continue
                    if isinstance(timestamp, datetime):
                        timestamp = timestamp.strftime("%Y-%m-%d %H:%M:%S")
                    values.append((
                        pair_map[key],
                        timestamp,
                        d.get("open_interest_contracts", 0.0),
                        d.get("open_interest_usd", 0.0),
                        d.get("funding_rate", 0.0),
                        d.get("volume_btc", 0.0),
                        d.get("volume_usd", 0.0),
                        d.get("price_usd", 0.0),
                        d.get("market_cap_usd", 0.0),
                    ))
                if not values:
                    self.logger.warning("Нет данных для сохранения в futures_data")
                    return
                self.logger.info(f"Попытка сохранить {len(values)} записей в futures_data")
                try:
                    await cur.executemany(query, values)
                    self.logger.info(f"Успешно сохранено {cur.rowcount} записей в futures_data")
                except Exception as e:
                    self.logger.error(f"Ошибка при сохранении данных: {str(e)}")
                    raise

    async def log_api_error(self, exchange: str, error_message: str):
        """Логирование ошибок API."""
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = """
                        INSERT INTO api_errors (exchange, error_message)
                        VALUES (%s, %s) \
                        """
                await cur.execute(query, (exchange, error_message))
                self.logger.info(f"Сохранена ошибка API для {exchange}")