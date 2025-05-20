import aiohttp
from typing import List, Dict
from utils.retry import retry_api


class CoinMarketCapClient:
    """Клиент для работы с CoinMarketCap API."""

    BASE_URL = "https://pro-api.coinmarketcap.com"

    def __init__(self, api_key: str):
        """Инициализация клиента."""
        self.api_key = api_key

    @retry_api()
    async def get_market_data(self, symbols: List[str]) -> Dict:
        """Получение рыночных данных."""
        headers = {"X-CMC_PRO_API_KEY": self.api_key}
        result = {}

        # Разбиение на группы по 200 символов
        for i in range(0, len(symbols), 200):
            batch = symbols[i:i + 200]
            params = {"symbol": ",".join(batch)}

            async with aiohttp.ClientSession() as session:
                async with session.get(
                        f"{self.BASE_URL}/v1/cryptocurrency/quotes/latest",
                        headers=headers,
                        params=params
                ) as resp:
                    data = await resp.json()
                    if "data" not in data:
                        continue
                    for symbol, info in data["data"].items():
                        result[symbol] = {
                            "price_usd": float(info["quote"]["USD"]["price"]),
                            "volume_usd": float(info["quote"]["USD"]["volume_24h"]),
                            "market_cap_usd": float(info["quote"]["USD"]["market_cap"])
                        }

        return result