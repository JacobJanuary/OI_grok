import asyncio
import logging
from datetime import datetime, timezone
from typing import List, Dict

from config.settings import Settings
from database.db import Database
from exchanges.binance import BinanceClient
from exchanges.bybit import BybitClient
from exchanges.gate_io import GateIOClient
from utils.logger import setup_logger


class FuturesCollector:
    """Класс для сбора данных о фьючерсных парах с бирж."""

    def __init__(self, settings: Settings):
        """Инициализация сборщика."""
        self.settings = settings
        self.db = Database(settings)
        self.clients = {
            "binance": BinanceClient(),
            "bybit": BybitClient(),
            #"gateio": GateIOClient(),
        }
        self.logger = logging.getLogger(__name__)

    async def run(self):
        """Запуск однократного сбора данных."""
        try:
            await self.db.connect()
            self.logger.info("Начало сбора данных")

            # Получение пар с бирж
            tasks = [
                self.clients[exchange].get_futures_pairs()
                for exchange in self.clients
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Сохранение токенов и пар
            all_base_symbols = set()
            for exchange, result in zip(self.clients.keys(), results):
                if isinstance(result, Exception):
                    self.logger.error(f"Ошибка при получении пар с {exchange}: {str(result)}")
                    await self.db.log_api_error(exchange, str(result))
                    continue

                exchange_pairs: List[Dict] = result
                self.logger.info(f"Получено {len(exchange_pairs)} пар с {exchange}")
                if not exchange_pairs:
                    self.logger.warning(f"Пустой список пар для {exchange}")
                    continue

                # Собираем base_symbol для сохранения в tokens
                base_symbols = [p["base_symbol"] for p in exchange_pairs]
                all_base_symbols.update(base_symbols)

            # Сохранение токенов перед парами
            if all_base_symbols:
                try:
                    await self.db.save_tokens(list(all_base_symbols))
                except Exception as e:
                    self.logger.error(f"Ошибка при сохранении токенов: {str(e)}")
                    await self.db.log_api_error("general", str(e))

            # Сохранение пар
            for exchange, result in zip(self.clients.keys(), results):
                if isinstance(result, Exception):
                    continue
                exchange_pairs: List[Dict] = result
                if not exchange_pairs:
                    continue
                try:
                    await self.db.save_futures_pairs(exchange, exchange_pairs)
                except Exception as e:
                    self.logger.error(f"Ошибка при сохранении пар для {exchange}: {str(e)}")
                    await self.db.log_api_error(exchange, str(e))

            # Получение данных по парам
            data_tasks = []
            for exchange in self.clients:
                pairs = await self.clients[exchange].get_futures_pairs()
                for pair in pairs:
                    data_tasks.append(
                        self.clients[exchange].get_pair_data(pair["symbol"], exchange)
                    )

            data_results = await asyncio.gather(*data_tasks, return_exceptions=True)

            # Сохранение данных
            futures_data = []
            for result in data_results:
                if isinstance(result, Exception):
                    self.logger.warning(f"Ошибка при получении данных: {str(result)}")
                    continue
                if result is not None:  # Пропускаем None
                    futures_data.append(result)

            self.logger.info(f"Собрано {len(futures_data)} валидных записей для сохранения")
            if futures_data:
                try:
                    await self.db.save_futures_data(futures_data)
                except Exception as e:
                    self.logger.error(f"Ошибка при сохранении данных: {str(e)}")
                    await self.db.log_api_error("general", str(e))
            else:
                self.logger.warning("Нет валидных данных для сохранения в futures_data")

            self.logger.info("Сбор данных завершён")

        except Exception as e:
            self.logger.error(f"Критическая ошибка: {str(e)}")
            raise
        finally:
            await self.db.close()

async def main():
    """Главная функция."""
    setup_logger()
    settings = Settings()
    collector = FuturesCollector(settings)
    await collector.run()

if __name__ == "__main__":
    asyncio.run(main())