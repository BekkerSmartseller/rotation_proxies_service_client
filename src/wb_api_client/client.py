# proxy_client_httpx.py
import asyncio
import json
import logging
import random
import time
from typing import Optional, Dict, List, Any, Set, Type, Literal, Union

import httpx
import msgspec
from msgspec import Struct
from redis import asyncio as aioredis
from redis import DataError

logger = logging.getLogger(__name__)

class GetTokens:
    def __init__(self, token_redis_url: str):
        self.token_redis_url = token_redis_url
        self.pool: Optional[aioredis.ConnectionPool] = None
        self.client: Optional[aioredis.Redis] = None

    async def connect(self) -> None:
        """Инициализация асинхронного подключения к Redis"""
        try:
            self.pool = aioredis.ConnectionPool.from_url(
                self.token_redis_url,
                max_connections=10,
                decode_responses=False,
                socket_connect_timeout=5,
                health_check_interval=30
            )
            self.client = aioredis.Redis(connection_pool=self.pool)
            await self.client.ping()
        except Exception as e:
            logger.error(f"Redis connection failed: {str(e)}")
            raise

    async def disconnect(self) -> None:
        """Корректное закрытие соединений"""
        if self.pool:
            try:
                await self.pool.disconnect()
            except Exception as e:
                logger.warning(f"Redis disconnect error: {str(e)}")

    @staticmethod
    def _decode_data(data: bytes) -> Dict[str, Any]:
        """Декодирование данных с валидацией структуры"""
        try:
            return msgspec.json.decode(data)
        except msgspec.DecodeError as e:
            logger.error(f"Token decoding failed: {str(e)}")
            raise ValueError("Invalid token format") from e

    async def fetch_tokens(self) -> List[Dict[str, str]]:
        """Получение и валидация токенов из Redis"""
        if not self.client:
            await self.connect()

        try:
            cursor = b'0'
            all_keys = []
            while cursor:
                cursor, keys = await self.client.scan(cursor, count=1000)
                all_keys.extend(keys)
            if not all_keys:
                raise DataError("No tokens found in Redis")

            tokens_data = await self.client.mget(all_keys)
            valid_tokens = []
            for data in tokens_data:
                if data is None:
                    continue
                try:
                    token = self._decode_data(data)
                    if self._validate_token(token):
                        valid_tokens.append(token)
                except ValueError:
                    continue

            if not valid_tokens:
                raise DataError("No valid tokens available")

            return valid_tokens

        except DataError as e:
            logger.error(f"Redis data error: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Token fetch failed: {str(e)}")
            raise

    @staticmethod
    def _validate_token(token: Dict) -> bool:
        """Валидация структуры токена"""
        required_fields = {'WBTokenV3', 'x-supplier-id', 'wbx-validation-key'}
        return all(field in token for field in required_fields)

    async def remove_token(self, token: Dict[str, str]) -> bool:
        """
        Удаление токена из Redis по значению
        
        Args:
            token: Токен для удаления (должен содержать WBTokenV3, x-supplier-id, wbx-validation-key)
            
        Returns:
            bool: True если токен был найден и удален, False если не найден
        """
        if not self.client:
            await self.connect()

        try:
            cursor = b'0'
            removed = False
            
            while cursor:
                cursor, keys = await self.client.scan(cursor, count=1000)
                for key in keys:
                    try:
                        data = await self.client.get(key)
                        if data is None:
                            continue
                            
                        existing_token = self._decode_data(data)
                        if (existing_token.get('WBTokenV3') == token.get('WBTokenV3') and
                            existing_token.get('x-supplier-id') == token.get('x-supplier-id') and
                            existing_token.get('wbx-validation-key') == token.get('wbx-validation-key')):
                            
                            await self.client.delete(key)
                            logger.info(f"Removed invalid token for supplier {token.get('x-supplier-id')}")
                            removed = True
                            break
                            
                    except (ValueError, msgspec.DecodeError):
                        continue
                if removed:
                    break
                    
            return removed
            
        except Exception as e:
            logger.error(f"Failed to remove token from Redis: {str(e)}")
            return False

class ProxyClientHTTPX:
    def __init__(
        self,
        proxy_service_url: str,
        api_secret: str,
        impersonate: str = "chrome99_android",  # для совместимости, игнорируется
        group_name: Optional[str] = None,
        priority_weights: Optional[Dict[str, int]] = None,
        token_redis_url: Optional[str] = None,
        token_refresh_interval: int = 300,
        **kwargs
    ):
        self.proxy_service_url = proxy_service_url
        self.headers = {"x-secret": api_secret}
        self.impersonate = impersonate  # не используется, но сохраняем для совместимости
        self.group_name = group_name
        self.priority_weights = priority_weights
        self.client: Optional[httpx.AsyncClient] = None
        self.params = self._build_params()

        # Инициализация менеджера токенов
        self.token_manager: Optional[GetTokens] = None
        self._token_cache: List[Dict] = []
        self.current_token_index = 0
        self.last_token_refresh: Optional[float] = None

        if token_redis_url:
            self.token_manager = GetTokens(token_redis_url)
            self.token_refresh_interval = token_refresh_interval
            self._token_lock = asyncio.Lock()

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def start(self):
        """Инициализация HTTPX клиента"""
        self.client = httpx.AsyncClient(
            headers=self.headers,
            timeout=httpx.Timeout(30.0, connect=10.0)  # базовые таймауты
        )

    async def close(self):
        """Закрытие клиента и соединений с Redis"""
        if self.client:
            await self.client.aclose()
        if self.token_manager:
            await self.token_manager.disconnect()

    def _build_params(self) -> dict:
        params = {}
        if self.group_name:
            params["group_name"] = self.group_name
        if self.priority_weights:
            params["priority_weights"] = json.dumps(self.priority_weights)
        return params

    async def _get_proxy(self) -> dict:
        """Получение прокси с сервиса (возвращает словарь с http и https ключами)"""
        try:
            response = await self.client.get(
                self.proxy_service_url,
                params=self.params
            )
            response.raise_for_status()
            data = response.json()
            return {
                "http": data["url"],
                "https": data["url"]
            }
        except Exception as e:
            logger.error(f"Failed to get proxy: {e}")
            raise

    # ---------- Методы работы с токенами (полностью скопированы из оригинального класса) ----------
    async def _refresh_tokens(self) -> None:
        async with self._token_lock:
            if self._should_refresh_tokens():
                try:
                    new_tokens = await self.token_manager.fetch_tokens()
                    if new_tokens:
                        self._token_cache = new_tokens
                        random.shuffle(self._token_cache)
                        self.current_token_index = 0
                        self.last_token_refresh = time.time()
                        logger.info(f"Refreshed tokens, total: {len(self._token_cache)}")
                    else:
                        logger.warning("No tokens available after refresh")
                except Exception as e:
                    logger.error(f"Token refresh failed: {str(e)}")
                    if not self._token_cache:
                        raise

    def _should_refresh_tokens(self) -> bool:
        if not self._token_cache:
            return True
        if self.last_token_refresh is None:
            return True
        elapsed = time.time() - self.last_token_refresh
        return elapsed > self.token_refresh_interval

    async def _get_next_token(self) -> Dict:
        await self._refresh_tokens()
        if not self._token_cache:
            raise ValueError("No available tokens")
        token = self._token_cache[self.current_token_index]
        self.current_token_index = (self.current_token_index + 1) % len(self._token_cache)
        return token

    async def _remove_current_token_and_retry(self, current_token: Dict[str, str]) -> bool:
        if not self.token_manager:
            return False
        try:
            logger.warning(f"Removing invalid token for supplier {current_token.get('x-supplier-id')}")
            removed = await self.token_manager.remove_token(current_token)
            if removed:
                async with self._token_lock:
                    self._token_cache = [t for t in self._token_cache
                                         if not (t.get('WBTokenV3') == current_token.get('WBTokenV3') and
                                                 t.get('x-supplier-id') == current_token.get('x-supplier-id') and
                                                 t.get('wbx-validation-key') == current_token.get('wbx-validation-key'))]
                    if self.current_token_index >= len(self._token_cache):
                        self.current_token_index = 0
                    logger.info(f"Token removed from cache. Remaining tokens: {len(self._token_cache)}")
                self.last_token_refresh = 0
                await self._refresh_tokens()
                return True
            else:
                logger.warning("Token not found in Redis for removal")
                return False
        except Exception as e:
            logger.error(f"Error removing token: {str(e)}")
            return False

    async def _handle_token_error(self, current_token: Dict[str, str], status_code: int, error_msg: str) -> bool:
        auth_errors = {401, 403, 419}
        if status_code in auth_errors:
            logger.warning(f"Auth error {status_code} with token for supplier {current_token.get('x-supplier-id')}")
            return await self._remove_current_token_and_retry(current_token)
        elif status_code == 400 and "token" in error_msg.lower():
            logger.warning(f"Token-related error {status_code}: {error_msg}")
            return await self._remove_current_token_and_retry(current_token)
        return False
    # --------------------------------------------------------------------------------------------

    async def request(
        self,
        method: str,
        url: str,
        response_type: Type[Struct] = None,
        strict: bool = True,
        type_return_data: Literal['objects', 'dict'] = 'objects',
        timeout: float = 3.0,
        max_retries: int = 3,
        no_retry_statuses: Set[int] = None,
        no_retry_classes: Set[int] = None,
        base_delay: float = 0.0,
        exponential_factor: float = 2.0,
        return_status_code: bool = False,
        return_cookies: bool = False,
        inject_wb_token: bool = False,
        **kwargs
    ) -> Union[tuple[int, Any], Union[Struct, dict, list, None], tuple[Any, dict], Any]:
        """
        Выполняет HTTP-запрос с поддержкой прокси, токенов и повторных попыток.
        Полностью совместим с оригинальным методом ProxyClientCFFI.request.
        """
        # Настройка декодера
        if response_type is not None:
            decoder = msgspec.json.Decoder(type=response_type, strict=strict)
        else:
            decoder = msgspec.json.Decoder()

        # Подготовка параметров (аналогично оригиналу)
        method = method.upper()
        if method in {"POST", "PUT", "PATCH"}:
            if "params" in kwargs:
                kwargs["data"] = msgspec.json.encode(kwargs.pop("params"))
                if "headers" not in kwargs:
                    kwargs["headers"] = {}
                kwargs["headers"].setdefault("Content-Type", "application/json")
        elif method in {"GET", "HEAD", "DELETE"}:
            if "params" in kwargs:
                params = kwargs.pop("params")
                kwargs["params"] = msgspec.to_builtins(params)
                kwargs["params"] = {k: v for k, v in kwargs["params"].items() if v is not None}

        # Статусные классы для исключённых статусов
        status_classes = {
            4: set(range(400, 500)),
            5: set(range(500, 562))
        }
        excluded_statuses = set()
        if no_retry_classes:
            for cls in no_retry_classes:
                excluded_statuses.update(status_classes.get(cls, set()))
        if no_retry_statuses:
            excluded_statuses.update(no_retry_statuses)

        last_cookies = None
        last_status_code = None
        current_token = None

        for attempt in range(1, max_retries + 1):
            delay = base_delay * (exponential_factor ** (attempt - 1))
            try:
                # Получаем прокси (может быть None, если сервис вернёт пустой URL?)
                proxy_dict = await self._get_proxy()
                proxy_url = proxy_dict.get("https") or proxy_dict.get("http")
                proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else None
                logger.info(f"Using proxy: {proxy_url} [Attempt {attempt}/{max_retries}]")

                # Добавляем токен если нужно
                if inject_wb_token:
                    if not self.token_manager:
                        raise ValueError("Token manager not initialized")
                    current_token = await self._get_next_token()
                    headers = kwargs.get('headers', {})
                    if current_token:
                        headers.update({
                            "Cookie": (
                                f"wbx-validation-key={current_token['wbx-validation-key']};"
                                f"WBTokenV3={current_token['WBTokenV3']};"
                                f"x-supplier-id-external={current_token['x-supplier-id']}"
                            ),
                            "authorizev3": current_token['WBTokenV3']
                        })
                        kwargs['headers'] = headers
                    else:
                        logger.warning("No valid tokens available for injection")

                # Выполняем запрос через httpx
                response = await self.client.request(
                    method=method,
                    url=url,
                    proxies=proxies,
                    timeout=timeout,
                    **kwargs
                )

                status_code = response.status_code
                last_status_code = status_code
                if return_cookies:
                    last_cookies = dict(response.cookies)

                # Если статус в excluded_statuses — возвращаем сразу без raise_for_status
                if status_code in excluded_statuses:
                    logger.warning(f"Received excluded status {status_code}")
                    # Проверяем ошибки авторизации даже для исключённых статусов
                    if inject_wb_token and current_token:
                        error_text = response.text
                        should_retry = await self._handle_token_error(current_token, status_code, error_text)
                        if should_retry and attempt < max_retries:
                            logger.info("Retrying with new token after auth error...")
                            await asyncio.sleep(delay)
                            continue
                    # Декодируем тело, если есть
                    content = response.content
                    result = decoder.decode(content) if content else None
                    return self._format_return(result, status_code, last_cookies, return_status_code, return_cookies)

                # Для остальных статусов проверяем на ошибку
                response.raise_for_status()

                # Успешный ответ
                content = response.content
                if response_type is not None and type_return_data == 'objects':
                    result = decoder.decode(content)
                elif response_type is not None and type_return_data == 'dict':
                    result = msgspec.to_builtins(decoder.decode(content))
                else:
                    result = decoder.decode(content)

                return self._format_return(result, status_code, last_cookies, return_status_code, return_cookies)

            except httpx.ProxyError as e:
                # Ошибка прокси (недоступен, неверный протокол и т.п.)
                status_code = 0
                error_msg = str(e)
                last_status_code = status_code
                logger.warning(f"Proxy error on attempt {attempt}: {error_msg}")
                if attempt < max_retries:
                    logger.info(f"Retrying in {delay:.1f}s...")
                    await asyncio.sleep(delay)

            except httpx.HTTPStatusError as e:
                status_code = e.response.status_code
                error_msg = e.response.text
                last_status_code = status_code
                if return_cookies:
                    last_cookies = dict(e.response.cookies)

                # Обработка ошибок токенов
                if inject_wb_token and current_token:
                    should_retry = await self._handle_token_error(current_token, status_code, error_msg)
                    if should_retry and attempt < max_retries:
                        logger.info("Retrying with new token after auth error...")
                        await asyncio.sleep(delay)
                        continue

                # Если статус в excluded_statuses — он уже был бы обработан выше, но здесь мы можем логировать
                logger.warning(f"HTTP error {status_code} on attempt {attempt}: {error_msg[:200]}")
                if attempt < max_retries:
                    logger.info(f"Retrying in {delay:.1f}s...")
                    await asyncio.sleep(delay)
                else:
                    # Если попытки кончились, возвращаем ошибку как результат
                    content = e.response.content
                    result = decoder.decode(content) if content else None
                    return self._format_return(result, status_code, last_cookies, return_status_code, return_cookies)

            except httpx.TimeoutException as e:
                status_code = 0
                error_msg = str(e)
                last_status_code = status_code
                logger.warning(f"Timeout on attempt {attempt}: {error_msg}")
                if attempt < max_retries:
                    logger.info(f"Retrying in {delay:.1f}s...")
                    await asyncio.sleep(delay)

            except httpx.RequestError as e:
                # Другие ошибки запроса (сетевая проблема, DNS и т.д.)
                status_code = 0
                error_msg = str(e)
                last_status_code = status_code
                logger.warning(f"Request error on attempt {attempt}: {error_msg}")
                if attempt < max_retries:
                    logger.info(f"Retrying in {delay:.1f}s...")
                    await asyncio.sleep(delay)

            except msgspec.DecodeError as e:
                logger.error(f"Decoding failed: {str(e)}")
                # Не повторяем, возвращаем None
                return self._format_return(None, last_status_code, last_cookies, return_status_code, return_cookies)

            except Exception as e:
                logger.error(f"Unexpected error: {str(e)}")
                raise

        # Если все попытки исчерпаны и не было успешного ответа
        return self._format_return(None, last_status_code, last_cookies, return_status_code, return_cookies)

    def _format_return(self, data, status_code, cookies, return_status, return_cookies):
        """Форматирование возвращаемого значения в соответствии с флагами."""
        if return_status and return_cookies:
            return (status_code or 0, data, cookies or {})
        if return_status:
            return (status_code or 0, data)
        if return_cookies:
            return (data, cookies or {})
        return data