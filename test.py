import polars as pl

df = pl.read_csv("./臺灣米價交易行情.csv", try_parse_dates=True)
