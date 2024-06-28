from datetime import datetime, timezone
from functools import partial
import json
import math
from pathlib import Path
import shutil
import sys
from basis_trading import utils
import ccxt
from loguru import logger
import pandas as pd
from pathlib import Path
import json
import click
import schedule
import time

from basis_trading.utils import supress, write_to_csv

OUTPUT_PATH = Path("outputs")

    
def get_data(api, binance_api):
    results = []
    for key, info in api.load_markets().items():
        with supress(exchange_id=api.id):
            if not info["future"] or not info["inverse"] or not info["active"]:
                continue
            if api.id == "deribit" and info["info"]["kind"] != "future":
                continue
                
            Path(f"infos/{api.id}_{info['id']}.json").write_text(json.dumps(info, indent=4))
            future_id = info["id"]
            future_price = api.fetch_ticker(future_id)["last"]
            if future_price is None:
                logger.info(f"{future_id=} Last price is none")
                continue
            
            base_symbol = info["settle"] + "USDT"
            base_price = binance_api.fetch_ticker(base_symbol)["last"]
            period_yield = (future_price - base_price) / base_price * 100
            expiry_date = datetime.fromisoformat(info["expiryDatetime"][:len("2024-06-28")])
            days_till_expiry = (expiry_date - datetime.now()).days
            days_till_expiry_fixed = max(days_till_expiry, 90)
            
            year_yield = period_yield
            if days_till_expiry > 90:
                year_yield = period_yield/ (days_till_expiry / 90)
            result = dict(exchange_id=api.id,
                id=key,
                future_id=future_id,
                base_symbol=base_symbol,
                expiry_date=expiry_date.strftime("%Y-%m-%d"),
                days_till_expiry=days_till_expiry,
                future_price=future_price,
                base_price=base_price,
                period_yield=round(period_yield, 2),
                year_yield=round(year_yield, 2)
            )
            results.append(result)
            logger.info(result)
    return results

            
def notify(threshold, result):
    if result["year_yield"] > threshold:
        result_str = "\n".join(f"{k}: {v}" for k, v in result.items())
        utils.message(result_str)

            
def iteration(threshold: float):
    # alpaca" # ccxt.base.errors.PermissionDenied: alpaca {"message": "forbidden."}
    # coinbase # ccxt.base.errors.AuthenticationError: coinbase requires "apiKey" credential
    # mercado # GET https://www.mercadobitcoin.net/api/coins 403 Forbidden
    # zaif # GET https://api.zaif.jp/api/1/currency_pairs/all 403 Forbidden 
    # binancecoinm # same as binance
    EXCHANGE_IDS = "binance bitget bitmex bybit deribit htx krakenfutures kucoinfutures okx".split()            
    
    binance_api = ccxt.binance()
    results = []
    for exchange_id in EXCHANGE_IDS:
        with supress(exchange_id=exchange_id):
            api = getattr(ccxt, exchange_id)()
            results += get_data(api=api, binance_api=binance_api)
    
    for result in results:
        notify(threshold=threshold, result=result)
        result["dt"] = datetime.now(timezone.utc).isoformat()
        file_name = f"{result['exchange_id']}_{result['future_id']}.csv"
        write_to_csv(file_path=OUTPUT_PATH / file_name, data=result)
        # pd.DataFrame(results).sort_values(["days_till_expiry", "period_yield"])


@click.command()
@click.option('-p', '--period', default=None, type=int, help='Retry period')
@click.option('-c', '--clean', default=False, is_flag=True, help='Clear outputs')
@click.option('-t', '--threshold', default=3, type=int, help='Notify threshold')
def main(period, clean, threshold):
    logger.remove()
    format = "{time:YYYY-MM-DD HH:mm:ss}|{name:10.10s}|{function:10.10s}|{level:4.4s}| {message}"
    logger.add(sys.stdout,  level='DEBUG', format=format)
    logger.add("log.log", level='DEBUG', format=format)
    logger.enable("")

    if clean:
        # remove dir with files
        shutil.rmtree(OUTPUT_PATH)
        OUTPUT_PATH.mkdir(exist_ok=True, parents=True)
    
    func = partial(iteration, threshold)        
    func()        

    if period:
        schedule.every(period).minutes.do(func)
        while True:
            secs = schedule.idle_seconds()
            logger.info(f"Sleeping for {secs} seconds")
            time.sleep(secs)
            schedule.run_pending()
                      
if __name__ == "__main__":
    main()