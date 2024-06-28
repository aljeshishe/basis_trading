from datetime import datetime, timedelta, timezone
import ccxt
import plotly.express as px
import logging
import pandas as pd
logging.basicConfig(level=logging.INFO, format='%(asctime)s|%(levelname)s|%(message)s')
log = logging.getLogger(__name__)


    
def print_futures(api, binance_api, results):
    for key, info in api.load_markets().items():
        with supress():
            if not info["future"] or not info["inverse"] or not info["active"]:
                continue
            if api.id == "deribit" and info["info"]["kind"] != "future":
                continue
                
            Path(f"infos/{api.id}_{info['id']}.json").write_text(json.dumps(info, indent=4))
            future_id = info["id"]
            future_price = api.fetch_ticker(future_id)["last"]
            if future_price is None:
                print(f"{future_id=} Last price is none")
                continue
            
            base_symbol = info["settle"] + "USDT"
            base_price = binance_api.fetch_ticker(base_symbol)["last"]
            period_yield = (future_price - base_price) / base_price * 100
            expiry_date = datetime.fromisoformat(info["expiryDatetime"][:len("2024-06-28")])
            days_till_expiry = (expiry_date - datetime.now()).days
            year_yield = period_yield / days_till_expiry * 365 if days_till_expiry else float("nan")
            result = dict(exchange_id=api.id,
                id=key,
                future_id=future_id,
                future_price=future_price,
                base_symbol=base_symbol,
                base_price=base_price,
                period_yield=period_yield,
                days_till_expiry=days_till_expiry,
                expiry_date=expiry_date,
                year_yield=year_yield
            )
            results.append(result)
            print(result)
def main():
    logger.remove()
    logger.add(sys.stderr,  level=main_cfg.log_level, format=main_cfg.log_format)
    logs_file_path = main_cfg.outputs_dir / "pipeline.log"
    logger.add(logs_file_path, level='DEBUG', format=main_cfg.log_format)
    logger.info(f"Writing logs to: {logs_file_path}")
    logger.enable("")

    # alpaca" # ccxt.base.errors.PermissionDenied: alpaca {"message": "forbidden."}
    # coinbase # ccxt.base.errors.AuthenticationError: coinbase requires "apiKey" credential
    # mercado # GET https://www.mercadobitcoin.net/api/coins 403 Forbidden
    # zaif # GET https://api.zaif.jp/api/1/currency_pairs/all 403 Forbidden 
    # binancecoinm # same as binance
    EXCHANGE_IDS = "binance bitget bitmex bybit deribit htx krakenfutures kucoinfutures okx".split()            

    from pathlib import Path
    import json
    binance_api = ccxt.binance()
    results = []
    for exchange_id in EXCHANGE_IDS:
        with supress():
            api = getattr(ccxt, exchange_id)()
            print_futures(api=api, binance_api=binance_api, results=results)

if __name__ == "__main__":
    main()