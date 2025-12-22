import marimo

__generated_with = "0.18.4"
app = marimo.App(width="medium", app_title="Discovery")


@app.cell
def _():
    import os

    import marimo as mo

    import json
    import polars as pl
    from datetime import datetime as dt
    return dt, mo, pl


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # 大麥克指數適合運用在亞洲嗎？
    米食文化區購買力新指標的探索。
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## ICP PPP 資料準備
    """)
    return


@app.cell
def _(pl):
    ppp_raw_df = pl.read_csv("./data/p_icp-2021-cycle/Data.csv")
    ppp_raw_df.head()
    return (ppp_raw_df,)


@app.cell
def _(ppp_raw_df):
    ppp_raw_df["Time"].unique()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    因為 ICP PPP 給了兩年的資料，我們待會只會選 2021 年的。
    """)
    return


@app.cell
def _(mo, ppp_raw_df):
    ppp_col_dropdown = mo.ui.dropdown(
        list(filter(lambda col: col[0].isdigit(), ppp_raw_df.columns)),
        value=list(filter(lambda col: "9260000" in col, ppp_raw_df.columns))[0],
        searchable=True,
    )
    mo.vstack([mo.md("選擇 ICP PPP 項目（建議：`9260000`）"), ppp_col_dropdown])
    return (ppp_col_dropdown,)


@app.cell
def _(pl, ppp_col_dropdown, ppp_raw_df):
    ppp_col = ppp_col_dropdown.value
    ppp_col_clean = ppp_col.split(":", 1)[0]

    ppp_df = (
        ppp_raw_df
            .filter(
                pl.col("Classification Code") == "PPPGlob",
                pl.col("Country Name").str.contains("(Benchmark)").not_(),
                pl.col(ppp_col).is_not_null(),
                pl.col("Time") == 2021
            )
            .select(
                pl.col("Country Name").alias("country_name"),
                pl.col("Country Code").alias("country_code"),
                pl.col(ppp_col).alias(ppp_col_clean)
            )
    ).unique()
    ppp_df
    return ppp_col_clean, ppp_df


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 大麥克指數資料準備
    """)
    return


@app.cell
def _(pl):
    bmac_raw_df = pl.read_csv("./data/big-mac-raw-index.csv", try_parse_dates=True)
    bmac_raw_df
    return (bmac_raw_df,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    相同的，我們待會也只需要 2021 年的資料。
    """)
    return


@app.cell
def _(bmac_raw_df, dt, pl):
    bmac_df = bmac_raw_df.filter(
        pl.col("date").dt.date() == dt(2021, 7, 1)
    )
    bmac_df
    return (bmac_df,)


@app.cell(hide_code=True)
def _(mo, ppp_col_clean):
    mo.md(rf"""
    以下便是整合 2021 年 ICP PPP 和大麥克指數的資料。
    `{ppp_col_clean}` 那一欄就是你選擇的 ICP PPP 項目。
    """)
    return


@app.cell
def _(bmac_df, pl, ppp_col_clean, ppp_df):
    df = (
        bmac_df
            .join(
                ppp_df,
                left_on="iso_a3",
                right_on="country_code",
                suffix="_right"
            )
            .select(
                pl.col("name"),
                pl.col("iso_a3").alias("country_code"),
                pl.col("currency_code"),
                pl.col("local_price"),
                pl.col(ppp_col_clean).cast(dtype=pl.Float64)
            )
    )
    df
    return (df,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 1. 以 ICP PPP 作為標準，探討大麥克指數於各地區準確度的差異
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    首先，我們需要知道大麥克在美國的價格。
    """)
    return


@app.cell
def _(bmac_df, mo, pl):
    bmac_usa_price = float(bmac_df.filter(pl.col("currency_code") == "USD")["local_price"][0])
    mo.md(f"大麥克在美國的價格：${bmac_usa_price}")
    return (bmac_usa_price,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    我們就可以算出各國的「大麥克購買力平價指數」（`bmac_ppp` 那一欄）。

    觀察以下資料，可以確認資料是正確的，因為美國的 `bmac_ppp` 為 `1`。
    """)
    return


@app.cell
def _(bmac_usa_price, df, pl):
    # step one dataframe
    one_df = df.with_columns(
        (pl.col("local_price") / bmac_usa_price).alias("bmac_ppp")
    )
    one_df.sort((pl.col("bmac_ppp") - 1).abs(), descending=False)
    return (one_df,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    為了方便我們待會觀察大麥克指數偏離情形最嚴重的國家，我們先載入國家地區資料。
    """)
    return


@app.cell
def _(pl):
    countries = pl.read_csv("./data/countries.csv")
    countries.head()
    return (countries,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    以下便是大麥克指數的偏離狀況，越上面的資料代表偏離（`bmac_index_deviation`）越明顯。
    """)
    return


@app.cell
def _(countries, one_df, pl, ppp_col_clean):
    deviation_df = (
        one_df
            .join(countries, left_on="country_code", right_on="Country Code")
            .select(
                pl.exclude(
                    list(
                        filter(
                            lambda col: col != "Country Code" and col != "Region", 
                            countries.columns
                        )
                    )
                )
            )
            .rename({"Region": "region"})
            .with_columns(
                ((pl.col("bmac_ppp") - pl.col(ppp_col_clean)) / pl.col(ppp_col_clean)).alias("bmac_index_deviation")
            )
    )

    # display
    deviation_df.select(
        pl.col("name"), 
        pl.col("local_price"), 
        pl.col(ppp_col_clean), 
        pl.col("region"), 
        pl.col("bmac_index_deviation")
    ).sort(pl.col("bmac_index_deviation").abs(), descending=True)
    return (deviation_df,)


@app.cell
def _(deviation_df, pl):
    group = deviation_df.select(pl.col("region"), pl.col("bmac_index_deviation").alias("mean_deviation")).group_by("region").mean()
    group.sort(pl.col("mean_deviation"), descending=True)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 2. 探討以米飯作為亞洲地區購買力的比較媒介之準確性
    """)
    return


if __name__ == "__main__":
    app.run()
