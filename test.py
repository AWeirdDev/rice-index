import marimo

__generated_with = "0.18.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import os
    import marimo as mo

    import json
    import polars as pl
    from polars import col
    from datetime import datetime as dt
    return col, mo, pl


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 準備 Global WFP food prices 2021 資料
    來源：聯合國世界糧食計畫署 https://data.humdata.org/dataset/global-wfp-food-prices
    """)
    return


@app.cell
def _(col, pl):
    food_prices = pl.read_csv("data/wfp_food_prices_global_2021.csv")
    food_prices = food_prices.remove(col("date")=="#date")
    return (food_prices,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 選擇米的種類
    """)
    return


@app.cell
def _(col, food_prices, mo):
    all_rice_types = food_prices.select(col("commodity")).filter(col("commodity").str.contains("Rice")).unique().sort(col("commodity"))
    rice_types = mo.ui.multiselect(
        list(all_rice_types)[0],
        value=list(all_rice_types)[0],
        label="選擇你想計算的米種: "
    )
    rice_types
    return (rice_types,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 查看有資料的國家
    """)
    return


@app.cell
def _(col, food_prices, rice_types):
    food_prices.select(
        "commodity",
        "countryiso3"
    ).filter(
        col("commodity").is_in(rice_types.value)
    ).unique(maintain_order=True)\
        .group_by(col("countryiso3")).all().sort(col("countryiso3"))
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 有什麼單位
    """)
    return


@app.cell
def _(col, food_prices, rice_types):
    food_prices.filter(
        col("commodity").is_in(rice_types.value)
    ).select(col("unit")).unique().sort(by="unit")
    return


if __name__ == "__main__":
    app.run()
