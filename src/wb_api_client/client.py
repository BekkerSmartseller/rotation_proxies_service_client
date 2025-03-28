from curl_cffi.requests import AsyncSession
from curl_cffi.requests.exceptions import ProxyError, RequestException
from typing import Optional, Dict, List, Any, Union, AsyncGenerator, Type,Set,Literal
from datetime import datetime, timedelta
import time
import random
import asyncio
import json
import coloredlogs, logging
import msgspec
from msgspec import Struct, field
from redis import asyncio as aioredis
from redis import  DataError


logger = logging.getLogger(__name__)
coloredlogs.install(level='DEBUG', logger=logger)
logging.basicConfig(
    format="%(asctime)-15s [%(levelname)s] %(funcName)s: %(message)s",
    level=logging.INFO)


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
            # Проверка подключения
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
            # Используем SCAN вместо KEYS 
            cursor = b'0'
            all_keys = []
            while cursor:
                cursor, keys = await self.client.scan(cursor, count=1000)
                all_keys.extend(keys)
            if not all_keys:
                raise DataError("No tokens found in Redis")

            # Пакетное получение значений
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



class ProxyClientCFFI:
    def __init__(
        self,
        proxy_service_url: str,
        api_secret: str,
        impersonate: Literal['chrome99_android', 'chrome133a', 'safari18_0', 'safari18_0_ios', 'firefox133'] = "chrome99_android",
        group_name: Optional[str] = None,
        priority_weights: Optional[Dict[str, int]] = None,
        token_redis_url: Optional[str] = None,
        token_refresh_interval: int = 300,
        **kwargs
    ):
        self.proxy_service_url = proxy_service_url
        self.headers = {"x-secret": api_secret}
        self.impersonate = impersonate
        self.group_name = group_name
        self.priority_weights = priority_weights
        self.session: Optional[AsyncSession] = None
        self.params = self._build_params()

        # Инициализация менеджера токенов
        self.token_manager: Optional[GetTokens] = None
        self._token_cache: List[Dict] = []
        self._token_index = 0
        
        if token_redis_url:
            self.token_manager = GetTokens(token_redis_url)
            self.token_refresh_interval = token_refresh_interval
            self._token_lock = asyncio.Lock()
            self._last_token_update: Optional[float] = None

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def start(self):
        self.session = AsyncSession(
            impersonate=self.impersonate,
            http_version=3,
            headers=self.headers
        )

    async def close(self):
        if self.session:
            await self.session.close()

    def _build_params(self) -> dict:
        params = {}
        if self.group_name:
            params["group_name"] = self.group_name
        if self.priority_weights:
            params["priority_weights"] = json.dumps(self.priority_weights)
        return params

    async def _get_proxy(self) -> dict:
        try:
            response = await self.session.get(
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
            logging.error(f"Failed to get proxy: {e}")
            raise

    async def _refresh_tokens(self) -> None:
        """Обновление токенов из Redis с блокировкой"""
        async with self._token_lock:
            if self._should_refresh_tokens():
                try:
                    # Используем существующий класс GetTokens или напрямую подключаемся к Redis
                    new_tokens = await self.token_manager.fetch_tokens()
                    if new_tokens:
                        self._token_cache = new_tokens
                        random.shuffle(self._token_cache)  # Перемешиваем для равномерного распределения
                        self.current_token_index = 0
                        self.last_token_refresh = datetime.now()
                        logger.info(f"Refreshed tokens, total: {len(self._token_cache)}")
                except Exception as e:
                    logger.error(f"Token refresh failed: {str(e)}")
                    if not self._token_cache:
                        raise

    def _should_refresh_tokens(self) -> bool:
        """Проверка необходимости обновления токенов"""
        if not self._token_cache:
            return True
        if self.last_token_refresh is None:
            return True
        elapsed = (datetime.now() - self.last_token_refresh).total_seconds()
        return elapsed > self.token_refresh_interval

    async def _get_next_token(self) -> Dict:
        """Получение следующего токена с автоматическим обновлением"""
        await self._refresh_tokens()

        if not self._token_cache:
            raise ValueError("No available tokens")

        # Циклический перебор токенов
        token = self._token_cache[self.current_token_index]
        self.current_token_index = (self.current_token_index + 1) % len(self._token_cache)
        return token


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
        Выполняет асинхронный HTTP-запрос с использованием ротируемых прокси.

        Параметры:
            method (str): HTTP-метод (GET, POST и т.д.)
            url (str): Целевой URL
            response_type (Type[Struct], optional): Класс для десериализации ответа
            strict (bool): Строгая валидация при десериализации
            type_return_data (str): Формат возвращаемых данных ('objects'/'dict')
            timeout (float): Таймаут запроса в секундах
            max_retries (int): Максимальное количество попыток
            no_retry_statuses (Set[int]): Статус-коды без повторных попыток
            no_retry_classes (Set[int]): Классы статус-кодов для исключения
            base_delay (float): Базовая задержка между попытками
            exponential_factor (float): Множитель экспоненциальной задержки
            return_status_code (bool): Если True, возвращает кортеж (status_code, data)
            return_cookies (bool): Если True, возвращает cookies в виде словаря
            inject_wb_token (bool): Если True добавляет в headers авторизацию WB токенов. Необходимо добавить token_redis_url
            **kwargs: Дополнительные параметры запроса

        Возвращает:
            При return_status_code=True и return_cookies=True: 
                tupleUnion[tuple[int, Any], Union[Struct, dict, list, None], Any] - (status_code, data, cookies)
            При return_status_code=True: возвращает кортеж (status_code, data)
                где status_code - HTTP-код ответа или 0 при ошибках подключения
                data - результат в указанном формате или None при ошибках
                tuple[int, Any]
            При return_cookies=True: 
                tuple[Any, dict]
            Иначе: Any

        Исключения:
            RequestException: При превышении максимального числа попыток
            msgspec.DecodeError: При ошибке десериализации
        """
        # Инициализация декодера
        if response_type is not None:
            decoder = msgspec.json.Decoder(type=response_type, strict=strict)
        else:
            decoder = msgspec.json.Decoder()
            # raw_data = msgspec.to_builtins(decoder.decode(raw_data_))
        
        # # Обработка параметров запроса
        method = method.upper()
        if method in {"POST", "PUT", "PATCH"}:
            # Кодируем тело запроса с помощью msgspec
            if "params" in kwargs:
                kwargs["data"] = msgspec.json.encode(kwargs.pop("params"))
                if "headers" not in kwargs:
                    kwargs["headers"] = {}
                kwargs["headers"].setdefault("Content-Type", "application/json")
        elif method in {"GET", "HEAD", "DELETE"}:
            # Кодируем параметры URL с помощью msgspec
            if "params" in kwargs:
                params = kwargs.pop("params")
                kwargs["params"] = msgspec.to_builtins(params)
                kwargs["params"] = {k: v for k, v in kwargs["params"].items() if v is not None}

        # Генерация статус-кодов для классов
        status_classes = {
            4: set(range(400, 500)),
            5: set(range(500, 562))
        }
        
        # Сбор исключенных статусов
        excluded_statuses = set()
        if no_retry_classes:
            for cls in no_retry_classes:
                excluded_statuses.update(status_classes.get(cls, set()))
        
        if no_retry_statuses:
            excluded_statuses.update(no_retry_statuses)
        
        last_cookies = None
        last_status_code = None
        last_exception = None
        # Основной цикл попыток
        for attempt in range(1, max_retries + 1):
            delay = base_delay * (exponential_factor ** (attempt - 1))
            try:
                proxy = await self._get_proxy()
                logging.info(f"Using proxy: {proxy} [Attempt {attempt}/{max_retries}]")
                
                if inject_wb_token:
                    if not self.token_manager:
                        raise ValueError("Token manager not initialized")
                    
                    token = await self._get_next_token()
                    headers = kwargs.get('headers', {})
                    # auth_headers = await self._get_auth_headers()
                    if token:
                        # Формируем заголовки авторизации
                        headers.update({
                            "Cookie": (
                                f"wbx-validation-key={token['wbx-validation-key']};"
                                f"WBTokenV3={token['WBTokenV3']};"
                                f"x-supplier-id-external={token['x-supplier-id']}"
                            ),
                            "authorizev3": token['WBTokenV3']
                        })
                        kwargs['headers'] = headers
                    else:
                        logger.warning("No valid tokens available for injection")

                # print(kwargs['headers'])

                response = await self.session.request(
                    method=method,
                    url=url,
                    proxies=proxy,
                    timeout=timeout,
                    **kwargs
                )
                
                status_code = response.status_code
                last_status_code = status_code
            
                # Обработка исключенных статусов
                if status_code in excluded_statuses:
                    logging.warning(f"Received excluded status {status_code}")
                    result = decoder.decode(response.content) if response.content else None
                    if return_status_code:
                        return (status_code, result)
                    return result
                
                response.raise_for_status()

                # Сохраняем cookies
                if return_cookies:
                    last_cookies = dict(response.cookies)

                # Обработка успешного ответа
                if response_type is not None and type_return_data == 'objects':
                    result = decoder.decode(response.content)
                elif response_type is not None and type_return_data == 'dict':
                    result = msgspec.to_builtins(decoder.decode(response.content))
                else:
                    result = decoder.decode(response.content)
                
                # if return_status_code:
                #     return (status_code, result)
                # return result
                return self._format_return(
                    result, 
                    status_code, 
                    last_cookies,
                    return_status_code,
                    return_cookies
                )

            except (ProxyError, RequestException) as e:
                # Получаем статус-код из объекта ответа
                if hasattr(e, "response") and e.response is not None:
                    last_status_code = e.response.status_code
                else:
                    # Парсим код из строки исключения для curl_cffi
                    error_str = str(e)
                    status_code = 0
                    if "HTTP Error" in error_str:
                        try:
                            status_code = int(error_str.split(":")[0].split()[-1])
                        except:
                            pass
                    last_status_code = status_code

                last_exception = e

                if last_status_code in excluded_statuses:
                    logging.warning(f"Excluded status {last_status_code} received")
                    break
                
                # Сохраняем cookies из response если есть
                if hasattr(e, "response") and e.response:
                    last_cookies = dict(e.response.cookies) if return_cookies else None

                logging.warning(f"Attempt {attempt} failed: {str(e)}")
                if attempt < max_retries:
                    logging.info(f"Retrying in {delay:.1f}s...")
                    await asyncio.sleep(delay)

            except msgspec.DecodeError as e:
                logging.error(f"Decoding failed: {str(e)}")
                last_exception = e
                break  # Прерываем цикл при ошибке десериализации

            except Exception as e:
                logging.error(f"Unexpected error: {str(e)}")
                last_exception = e
                raise

        # Обработка результата после всех попыток
        # if return_status_code:
        #     return (last_status_code or 0, None)
        
        # if last_exception:
        #     raise last_exception

        # Обработка после всех попыток
        return self._format_return(
            None, 
            last_status_code, 
            last_cookies,
            return_status_code,
            return_cookies
        )        
        # raise RequestException(f"Max retries ({max_retries}) exceeded")
    def _format_return(self, data, status_code, cookies, return_status, return_cookies):
        """Форматирует возвращаемое значение в зависимости от флагов"""
        
        if return_status and return_cookies:
            return (status_code or 0, data, cookies or {})
        if return_status:
            return (status_code or 0, data)
        if return_cookies:
            return (data, cookies or {})
        return data

