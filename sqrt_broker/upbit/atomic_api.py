import asyncio
import jwt
import hashlib
import os
import requests
import uuid
import pandas as pd
import numpy as np
from urllib.parse import urlencode, unquote

from sqrt_broker.utils.api_throttle import UpbitOrderAPIMinutelyThrottle
from sqrt_broker.utils.api_throttle import UpbitOrderAPISecondlyThrottle
from sqrt_broker.utils.api_throttle import UpbitQuotationAPISecondlyThrottle
from sqrt_broker.utils.api_throttle import UpbitExchangeAPISecondlyThrottle
from sqrt_broker.utils.broker_exceptions import APIException

from sqrt_broker.upbit.upbit_broker_logger import get_upbit_broker_logger

from sqrt_broker.utils.load_environment import load_env

logger = get_upbit_broker_logger()

load_env(".api")

order_api_minutely_throttle = UpbitOrderAPIMinutelyThrottle()
order_api_secondly_throttle = UpbitOrderAPISecondlyThrottle()
order_api_throttles = {
    "order_api_minutely_throttle": order_api_minutely_throttle,
    "order_api_secondly_throttle": order_api_secondly_throttle,
}

quotation_api_secondly_throttle = UpbitQuotationAPISecondlyThrottle()
exchange_api_secondly_throttle = UpbitExchangeAPISecondlyThrottle()

timeout = 10
retry_limit = 5


def order_api_throttling(func):
    async def wrapper(*args, **kwargs):
        throttle_start_info = {}
        for throttle_name, throttle in order_api_throttles.items():
            throttle_start_info[throttle_name] = await throttle.insert_job(
                f"{str(func)}{str(args)}{str(kwargs)}"
            )

        try:
            ret = func(*args, **kwargs)
        except Exception as e:
            for throttle_name, throttle in order_api_throttles.items():
                throttle.job_done(*throttle_start_info[throttle_name])
            raise e

        for throttle_name, throttle in order_api_throttles.items():
            throttle.job_done(*throttle_start_info[throttle_name])

        return ret

    return wrapper


def quotation_api_throttling(func):
    async def wrapper(*args, **kwargs):
        start_info = await quotation_api_secondly_throttle.insert_job(
            f"{str(func)}{str(args)}{str(kwargs)}"
        )

        try:
            ret = func(*args, **kwargs)
        except Exception as e:
            quotation_api_secondly_throttle.job_done(*start_info)
            raise e

        quotation_api_secondly_throttle.job_done(*start_info)
        return ret

    return wrapper


def exchange_api_throttling(func):
    async def wrapper(*args, **kwargs):
        start_info = await exchange_api_secondly_throttle.insert_job(
            f"{str(func)}{str(args)}{str(kwargs)}"
        )
        try:
            ret = func(*args, **kwargs)
        except Exception as e:
            exchange_api_secondly_throttle.job_done(*start_info)
            raise e

        exchange_api_secondly_throttle.job_done(*start_info)
        return ret

    return wrapper


def async_retry(async_func):
    async def wrapper(*args, **kwargs):
        for i in range(retry_limit):
            try:
                ret = await async_func(*args, **kwargs)
                return ret
            except Exception as e:
                logger.info(f"Retry! Error in step {i}\n" f"{e}\n")
                last_exception = e
                await asyncio.sleep(1)
                continue
        raise last_exception

    return wrapper


access_key = os.environ["UPBIT_OPEN_API_ACCESS_KEY"]
secret_key = os.environ["UPBIT_OPEN_API_SECRET_KEY"]
server_url = os.environ["UPBIT_OPEN_API_SERVER_URL"]


@async_retry
@order_api_throttling
def create_market_ask_order(symbol, count, base_currency="KRW"):
    logger.info(f"create_market_ask_order({symbol}, {count}, {base_currency})")

    market = f"{base_currency}-{symbol}"
    params = {
        "market": market,
        "side": "ask",
        "ord_type": "market",
        "volume": str(count),
    }
    headers = encrypt_header(params)

    res = requests.post(
        server_url + "/v1/orders", json=params, headers=headers, timeout=timeout
    )

    if res.status_code // 100 == 2:
        return res.json()
    else:
        err_msg = (
            f"request fail in create_market_ask_order({symbol}, {count}, {base_currency})\n"
            f"param: {params}\n"
            f"response status: {res.status_code}\n"
            f"text: {res.text}\n"
            f"header: {res.headers}\n"
        )
        logger.warning(err_msg)

        raise APIException(err_msg)


@async_retry
@order_api_throttling
def create_market_bid_order(symbol, value, base_currency="KRW"):
    logger.info(f"create_market_bid_order({symbol}, {value}, {base_currency})")

    market = f"{base_currency}-{symbol}"
    params = {
        "market": market,
        "side": "bid",
        "ord_type": "price",
        "price": str(value),
    }
    headers = encrypt_header(params)

    res = requests.post(
        server_url + "/v1/orders", json=params, headers=headers, timeout=timeout
    )

    if res.status_code // 100 == 2:
        return res.json()
    else:
        err_msg = (
            f"request fail in create_market_bid_order({symbol}, {value}, {base_currency})\n"
            f"param: {params}\n"
            f"response status: {res.status_code}\n"
            f"text: {res.text}\n"
            f"header: {res.headers}\n"
        )
        logger.warning(err_msg)

        raise APIException(err_msg)


@async_retry
@order_api_throttling
def create_limit_order(symbol, side, count, price, base_currency="KRW"):
    logger.info(
        f"create_limit_order({symbol}, {side}, {count}, {price}, {base_currency})"
    )

    market = f"{base_currency}-{symbol}"
    params = {
        "market": market,
        "side": side,
        "ord_type": "limit",
        "price": str(price),
        "volume": str(count),
    }
    headers = encrypt_header(params)

    res = requests.post(
        server_url + "/v1/orders", json=params, headers=headers, timeout=timeout
    )

    if res.status_code // 100 == 2:
        return res.json()
    else:
        err_msg = (
            f"request fail in create_limit_order({symbol}, {side}, {count}, {price}, {base_currency})\n"
            f"param: {params}\n"
            f"response status: {res.status_code}\n"
            f"text: {res.text}\n"
            f"header: {res.headers}\n"
        )
        logger.warning(err_msg)

        raise APIException(err_msg)


@async_retry
@order_api_throttling
def delete_order(target_uuid):
    logger.info(f"delete_order({target_uuid})")

    params = {"uuid": target_uuid}
    headers = encrypt_header(params)

    res = requests.delete(
        server_url + "/v1/order", params=params, headers=headers, timeout=timeout
    )

    if res.status_code // 100 == 2:
        return res.json()
    else:
        err_msg = (
            f"request fail in delete_order({uuid})\n"
            f"param: {params}\n"
            f"response status: {res.status_code}\n"
            f"text: {res.text}\n"
            f"header: {res.headers}\n"
        )
        logger.warning(err_msg)

        raise APIException(err_msg)


@async_retry
@exchange_api_throttling
def get_orders_waiting(symbol, base_currency="KRW"):
    logger.info(f"get_orders_waiting({symbol}, {base_currency})")

    params = {"states[]": ["wait"], "market": f"{base_currency}-{symbol}", "pages": 0}

    if symbol is None:
        params.pop("market")

    headers = encrypt_header(params)

    waiting_orders = []
    while True:
        res = requests.get(
            server_url + "/v1/orders", params=params, headers=headers, timeout=timeout
        )

        if res.status_code // 100 == 2:
            orders_in_page = res.json()
            waiting_orders += orders_in_page
            params["pages"] += 1

            if len(orders_in_page) < 100:
                break
        else:
            err_msg = (
                f"request fail in get_orders_waiting({symbol}, {base_currency})\n"
                f"param: {params}\n"
                f"response status: {res.status_code}\n"
                f"text: {res.text}\n"
                f"header: {res.headers}\n"
            )
            logger.warning(err_msg)

            raise APIException(err_msg)

    return waiting_orders


@async_retry
@exchange_api_throttling
def get_my_asset_status():
    logger.info(f"get_my_asset_status()")

    payload = {
        "access_key": access_key,
        "nonce": str(uuid.uuid4()),
    }

    headers = {
        "Authorization": "Bearer {}".format(jwt.encode(payload, secret_key)),
    }

    res = requests.get(server_url + "/v1/accounts", headers=headers, timeout=timeout)

    if res.status_code // 100 == 2:
        return res.json()
    else:
        err_msg = (
            f"request fail in get_my_asset_status\n"
            f"response status: {res.status_code}\n"
            f"text: {res.text}\n"
            f"header: {res.headers}\n"
        )
        logger.warning(err_msg)

        raise APIException(err_msg)


@async_retry
@quotation_api_throttling
def get_current_orderbook(symbol, base_currency="KRW"):
    logger.info(f"get_current_orderbook({symbol}, {base_currency})")

    url = f"https://api.upbit.com/v1/orderbook?markets={base_currency}-{symbol}"
    headers = {"accept": "application/json"}
    res = requests.get(url, headers=headers, timeout=timeout)

    if res.status_code // 100 == 2:
        return res.json()
    else:
        err_msg = (
            f"request fail in get_current_orderbook({symbol}, {base_currency})\n"
            f"response status: {res.status_code}\n"
            f"text: {res.text}\n"
            f"header: {res.headers}\n"
        )
        logger.warning(err_msg)

        raise APIException(err_msg)


# Not in use yet.
# def get_market_info(symbol, base_currency="KRW"):
#     logger.info(f"get_market_info({symbol}, {base_currency})")
#
#     params = {"market": f"{base_currency}-{symbol}"}
#     headers = encrypt_header(params)
#
#     res = requests.get(server_url + "/v1/orders/chance", params=params, headers=headers, timeout=timeout)
#
#     if res.status_code // 100 == 2:
#         return res.json()
#     else:
#         err_msg = (
#             f"request fail in get_market_info({symbol}, {base_currency})\n"
#             f"response status: {res.status_code}\n"
#             f"text: {res.text}\n"
#             f"header: {res.headers}\n"
#         )
#         logger.warning(err_msg)
#
#         raise APIException(err_msg)


@async_retry
@quotation_api_throttling
def get_latest_10min_vwap(symbol, base_currency="KRW"):
    logger.info(f"get_latest_10min_vwap({symbol}, {base_currency})")

    url = f"https://api.upbit.com/v1/candles/minutes/10?market={base_currency}-{symbol}&count=2"
    headers = {"accept": "application/json"}
    res = requests.get(url, headers=headers, timeout=timeout)

    if res.status_code // 100 == 2:
        candle_match_the_time = [
            i
            for i in res.json()
            if pd.Timestamp(i["candle_date_time_utc"])
            == (pd.Timestamp("now").floor("10min") - pd.Timedelta(minutes=10))
        ]

        if len(candle_match_the_time) != 1:
            err_msg = (
                f"No candle matching timestamp in get_latest_10min_vwap({symbol}, {base_currency})\n"
                f"response status: {res.status_code}\n"
                f"text: {res.text}\n"
                f"header: {res.headers}\n"
            )
            logger.warning(err_msg)

            raise APIException(err_msg)
        else:
            candle_match_the_time = candle_match_the_time[0]
            return np.float64(
                candle_match_the_time["candle_acc_trade_price"]
            ) / np.float64(candle_match_the_time["candle_acc_trade_volume"])

    else:
        err_msg = (
            f"request fail in get_latest_10min_vwap({symbol}, {base_currency})\n"
            f"response status: {res.status_code}\n"
            f"text: {res.text}\n"
            f"header: {res.headers}\n"
        )
        logger.warning(err_msg)

        raise APIException(err_msg)


def encrypt_header(params):
    query_string = unquote(urlencode(params, doseq=True)).encode("utf-8")

    m = hashlib.sha512()
    m.update(query_string)
    query_hash = m.hexdigest()

    payload = {
        "access_key": access_key,
        "nonce": str(uuid.uuid4()),
        "query_hash": query_hash,
        "query_hash_alg": "SHA512",
    }

    jwt_token = jwt.encode(payload, secret_key)
    authorization = "Bearer {}".format(jwt_token)
    headers = {
        "Authorization": authorization,
    }

    return headers
