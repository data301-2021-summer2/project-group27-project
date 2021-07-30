import pandas as pd
import glob
import re


def load_data():
    files = glob.glob("../data/raw" + "/*.csv")
    return pd.concat(
        [pd.read_csv(filename, index_col=None, header=0) for filename in files],
        axis=0,
        ignore_index=True,
    )


def create_volume_asset(df):
    if "volume_asset" not in df:
        df["volume_asset"] = 0
        rexp = r"Volume\s(?!USDT)"
        for col in df.columns:
            if re.search(rexp, col) is not None:
                new_col = pd.DataFrame({"volume_asset": df[col]})
                df.update(new_col)
                df = df.drop(columns=[col])
    return df


def load_and_process():
    data = (
        load_data()
        .rename(columns={"Volume USDT": "volume_usdt", "tradecount": "trade_count"})
        .pipe(create_volume_asset)
    )
    data = data.assign(
        date=lambda x: pd.to_datetime(x["date"]),
        asset_coin=lambda x: [symbol.split("/")[0] for symbol in x["symbol"]],
        base_coin=lambda x: [symbol.split("/")[1] for symbol in x["symbol"]],
        avg_trade_asset=lambda x: x["volume_asset"] / x["trade_count"],
        avg_trade_usdt=lambda x: x["volume_usdt"] / x["trade_count"],
        delta_value=lambda x: (x["close"] - x["open"]) / x["open"],
        delta_range=lambda x: x["high"] - x["low"],
    )
    data = data.drop(columns=["symbol", "unix", "high", "low", "close"])
    return data
