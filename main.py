from datetime import datetime, date, time
from copy import deepcopy
import pandas as pd
from typing import NamedTuple
import argparse
import sys
import uuid
import random

if sys.version_info[0] < 3:
    from StringIO import StringIO
else:
    from io import StringIO


def find_frames_in_csv(file):

    start_frame = "BOS"
    end_frame = "EOS"
    frames = []

    with open(file) as in_file:
        ct = 0
        for num, line in enumerate(in_file, 1):
            if line.strip().split("|")[0] == start_frame:
                frames.append(
                    dict(
                        name=line.strip().split("|")[2],
                        label=line.strip().split("|")[1],
                        bos_line=num,
                        eos_line=0,
                    )
                )
            if line.strip().split("|")[0] == end_frame:
                frames[ct]["eos_line"] = num
                ct += 1

    return frames


def export_from_csv(csv_file, frame):
    header = frame["bos_line"]
    nrows = frame["eos_line"] - header - 2
    raw_reports = pd.read_csv(csv_file, sep="|", header=header, nrows=nrows)
    return raw_reports


def search(label, frames):
    return [element for element in frames if element["label"] == label][0]


def merge_tables(result_table, table1, table2, iteration=0):
    iteration += 1
    if (
        len(table1[table1["Open/CloseIndicator"].isin(["O", "C"])].index) == 0
        or len(table2[table2["Open/CloseIndicator"].isin(["O", "C"])].index) == 0
    ):
        return result_table, table1, table2
    else:
        table1, table2 = table2, table1
        table1_copy = table1.copy(deep=True)
        for row1 in table1_copy.iterrows():
            if (
                row1[1]["Open/CloseIndicator"] != "O"
                and row1[1]["Open/CloseIndicator"] != "C"
            ):
                continue
            row2_o_t = table2[
                (table2.Symbol == row1[1]["Symbol"])
                & (table2["Buy/Sell"] != row1[1]["Buy/Sell"])
                & (
                    (row1[1]["Open/CloseIndicator"] == "O")
                    & (row1[1]["Date"] <= table2["Date"])
                    | (row1[1]["Open/CloseIndicator"] == "C")
                    & (row1[1]["Date"] >= table2["Date"])
                )
            ]
            # if row1[1]["Symbol"] == "VWO":
            # print("ROW1")
            # print(row1)
            # print("ROW2_OT")
            # print(row2_o_t)
            if row2_o_t.empty:
                row1[1]["Open/CloseIndicator"] += ";P"
                table1.loc[row1[0]] = row1[1]
            else:
                row2_o = row2_o_t[
                    (abs(row2_o_t["Quantity"]) <= abs(row1[1]["Quantity"]))
                ]

                if row2_o.empty:
                    continue

                if not row2_o_t[
                    (abs(row2_o_t["Quantity"]) > abs(row1[1]["Quantity"]))
                    & (row2_o_t.index < row2_o.index[0])
                ].empty:
                    continue

                copy_row1 = deepcopy(row1)
                row2 = (
                    row2_o.loc[row2_o.index[0]]
                    if copy_row1[1]["Open/CloseIndicator"] == "O"
                    else row2_o.loc[row2_o.index[-1]]
                )
                op_id = (
                    row2_o.loc[row2_o.index[0]]["Date"].to_pydatetime().timestamp()
                    if copy_row1[1]["Open/CloseIndicator"] == "O"
                    else row2_o.loc[row2_o.index[-1]]["Date"]
                    .to_pydatetime()
                    .timestamp()
                ) + copy_row1[1]["Date"].to_pydatetime().timestamp() * random.randint(
                    10, 100
                )
                result_table = result_table.append(row2, ignore_index=True)
                result_table.at[result_table.index[-1], "Op_ID"] = op_id

                oc_ref = abs(row2["Quantity"] / row1[1]["Quantity"])

                row1[1]["Quantity"] = -1 * row2["Quantity"]
                row1[1]["Proceeds"] = oc_ref * row1[1]["Proceeds"]
                row1[1]["IBCommission"] = oc_ref * row1[1]["IBCommission"]

                pl = (
                    row1[1]["Proceeds"]
                    + row2["Proceeds"]
                    + row1[1]["IBCommission"]
                    + row2["IBCommission"]
                )
                result_table.at[result_table.index[-1], "PL"] = pl
                result_table = result_table.append(row1[1], ignore_index=True)
                result_table.at[result_table.index[-1], "PL"] = pl
                result_table.at[result_table.index[-1], "Op_ID"] = op_id
                if copy_row1[1]["Open/CloseIndicator"] == "O":
                    table2 = table2.drop(row2_o.index[0])
                else:
                    table2 = table2.drop(row2_o.index[-1])
                copy_row1[1]["Quantity"] -= row1[1]["Quantity"]
                copy_row1[1]["Proceeds"] -= row1[1]["Proceeds"]
                copy_row1[1]["IBCommission"] -= row1[1]["IBCommission"]
                if copy_row1[1]["Quantity"] == 0:
                    table1 = table1.drop(copy_row1[0])
                else:
                    table1.at[copy_row1[0], "Quantity"] = copy_row1[1]["Quantity"]
                    table1.at[copy_row1[0], "Proceeds"] = copy_row1[1]["Proceeds"]
                    table1.at[copy_row1[0], "IBCommission"] = copy_row1[1][
                        "IBCommission"
                    ]

                if oc_ref > 0:
                    break

        return merge_tables(result_table, table1, table2, iteration)


def merge_orphaned_tables(open_operations, close_operations):

    open_close_df = pd.DataFrame(
        columns=[
            "Symbol",
            "Open/CloseIndicator",
            "Date",
            "Buy/Sell",
            "Quantity",
            "CurrencyPrimary",
            "TradePrice",
            "Proceeds",
            "PL",
        ]
    )
    return merge_tables(open_close_df, open_operations, close_operations)


def create_pl_table(pd_frames):
    groupped_pd = pd_frames.groupby(
        by=[
            "Symbol",
            "Date",
            "Buy/Sell",
            # "TradePrice",
            "CurrencyPrimary",
            "Open/CloseIndicator",
        ],
        as_index=False,
    )[["Quantity", "Proceeds", "IBCommission"]].sum()
    # groupped_pd.to_excel("{}.xlsx".format(uuid.uuid4()))

    open_operations = groupped_pd.loc[groupped_pd["Open/CloseIndicator"] == "O"]
    close_operations = groupped_pd.loc[groupped_pd["Open/CloseIndicator"] == "C"]

    open_close_df = pd.DataFrame(
        columns=[
            "Symbol",
            "Open/CloseIndicator",
            "Date",
            "Buy/Sell",
            "Quantity",
            "CurrencyPrimary",
            # "TradePrice",
            "IBCommission",
            "Proceeds",
            "PL",
            "Op_ID",
        ]
    )
    oc_df, df_1, df_2 = merge_tables(open_close_df, open_operations, close_operations)

    return oc_df, df_1, df_2


def setter(x, key, val):
    x[[key]] = val
    return x


def create_currency_table_bs(courses_file, df):
    currency_table = (
        pd.read_excel(courses_file)
        .rename(index=str, columns={"curs": "CB_course", "data": "Date"})
        .filter(items=["Date", "CB_course"])
    )

    pl_table = pd.merge(df, currency_table, on="Date", sort=False, how="left")
    pl_table[["CB_course", "Date"]] = pl_table[["CB_course", "Date"]].apply(
        lambda x: x
        if x["CB_course"] > 0
        else setter(
            x,
            "CB_course",
            currency_table.iloc[
                [
                    (
                        currency_table.Date[currency_table.Date <= x["Date"]]
                        - x["Date"]
                    ).idxmax()
                ]
            ]["CB_course"],
        ),
        axis=1,
    )
    pl_table["Cash_Rub"] = (
        pl_table["Cash"] * pl_table["CB_course"] if "Cash" in pl_table.columns else 0
    )
    pl_table["PL_Rub"] = 0
    if "ActivityCode" not in pl_table.columns:
        pl_table = pl_table.apply(
            lambda x: setter(
                x, "PL_Rub", x["Cash_Rub"] + pl_table.iloc[x.name + 1]["Cash_Rub"],
            )
            if x.name % 2 == 0
            else setter(
                x, "PL_Rub", x["Cash_Rub"] + pl_table.iloc[x.name - 1]["Cash_Rub"],
            ),
            axis=1,
        )
    else:
        pl_table["PL_Rub"] = pl_table["CB_course"] * pl_table["PL"]

    return pl_table


def export_frame_from_csv(csv_file, frame_name):

    frame_items = {
        "TRNT": [
            "Symbol",
            "TradeDate",
            "Buy/Sell",
            "CurrencyPrimary",
            "Quantity",
            # "TradePrice",
            "IBCommission",
            "Proceeds",
            "Open/CloseIndicator",
        ],
        "STFU": ["Symbol", "ActivityCode", "Date", "CurrencyPrimary", "Amount"],
    }

    frames = find_frames_in_csv(csv_file)

    return export_from_csv(csv_file, search(frame_name, frames)).filter(
        items=frame_items[frame_name]
    )


def count_trn_pl(this_year_file, currency_courses_file, prev_year_file=""):

    this_year_df = export_frame_from_csv(this_year_file, "TRNT").rename(
        index=str, columns={"TradeDate": "Date"}
    )
    this_year_df["Date"] = pd.to_datetime(this_year_df["Date"], format="%Y%m%d")
    this_year_df = this_year_df[(abs(this_year_df["Proceeds"])) > 0]

    this_year_pl, df_1, df_2 = create_pl_table(this_year_df)
    prev_year_pl = pd.DataFrame()

    if len(prev_year_file) > 0:
        prev_year_df = pd.DataFrame()

        for file in prev_year_file:
            prev_year_df = prev_year_df.append(
                export_frame_from_csv(file, "TRNT").rename(
                    index=str, columns={"TradeDate": "Date"}
                )
            )
        print(df_1)
        print(df_2)
        prev_year_df["Date"] = pd.to_datetime(prev_year_df["Date"], format="%Y%m%d")
        prev_year_df = prev_year_df[(abs(prev_year_df["Proceeds"])) > 0]
        prev_pl, prev_year_pl_df_1, prev_year_pl_df_2 = create_pl_table(prev_year_df)
        additional_trans_df = pd.concat(
            [
                prev_year_pl_df_1[prev_year_pl_df_1["Open/CloseIndicator"] == "O;P"][
                    ::-1
                ],
                prev_year_pl_df_2[prev_year_pl_df_2["Open/CloseIndicator"] == "O;P"][
                    ::-1
                ],
                df_1[df_1["Open/CloseIndicator"] == "C;P"],
                df_1[df_1["Open/CloseIndicator"] == "C"],
                df_2[df_2["Open/CloseIndicator"] == "C;P"],
                df_2[df_2["Open/CloseIndicator"] == "C"],
            ]
        ).reset_index(drop=True)

        additional_trans_df.loc[
            additional_trans_df["Open/CloseIndicator"] == "C;P", "Open/CloseIndicator"
        ] = "C"
        additional_trans_df.loc[
            additional_trans_df["Open/CloseIndicator"] == "O;P", "Open/CloseIndicator"
        ] = "O"
        print(additional_trans_df)
        prev_year_pl, df_1, df_2 = create_pl_table(additional_trans_df)
    print(additional_trans_df)
    pl_df = pd.concat([prev_year_pl, this_year_pl])
    if not pl_df.empty:
        pl_df["Cash"] = pl_df["IBCommission"] + pl_df["Proceeds"]

        pl = create_currency_table_bs(courses_file=currency_courses_file, df=pl_df)

        finish_pl = pl[
            [
                "Symbol",
                "Date",
                "CurrencyPrimary",
                "Buy/Sell",
                "Quantity",
                # "TradePrice",
                "PL",
                "IBCommission",
                "Cash",
                "CB_course",
                "Cash_Rub",
                "PL_Rub",
                "Op_ID",
            ]
        ].rename(
            index=str,
            columns={
                "Symbol": "Актив",
                "Date": "Дата",
                "CurrencyPrimary": "Валюта",
                "Buy/Sell": "Сделка",
                "Quantity": "Кол-во",
                # "TradePrice": "Стоимость позиции",
                "CB_course": "Курс руб. ЦБРФ",
                "PL_Rub": "Прибыль / Убыток, руб",
                "PL": "Прибыль / Убыток, Валюта",
                "Cash_Rub": "Кон.сумма сделки, руб",
                "IBCommission": "Комиссия, Валюта",
                "Cash": "Кон.сумма сделки, Валюта",
            },
        )

        return finish_pl
    return pl_df


def create_div_table(df):
    divs_table = pd.DataFrame(
        columns=["Symbol", "Date", "CurrencyPrimary", "Amount", "Tax", "PL"]
    )

    for row in df[df["ActivityCode"].isin(["DIV", "PIL"])].iterrows():
        div_row = deepcopy(row)
        div_row[1]["Tax"] = 0

        find_tax = df[
            (df["ActivityCode"] == "FRTAX")
            & (df["Symbol"] == row[1]["Symbol"])
            & (df["Date"] == row[1]["Date"])
        ]

        if not find_tax.empty:
            div_row[1]["Tax"] = find_tax.loc[find_tax.index[0]]["Amount"]
            df = df.drop(find_tax.index[0])

        divs_table = divs_table.append(div_row[1])
    divs_table["PL"] = divs_table["Amount"] + divs_table["Tax"]
    return divs_table


def count_tax_debt(df, final_tax):

    df["Tax_percent"] = abs(df["Tax"] / df["Amount"]) * 100
    df["Tax_RUB"] = df["Tax"] * df["CB_course"]

    df["Tax_to_pay_RUB"] = (
        df[df["Tax_percent"] <= float(final_tax)]["Tax_RUB"]
        * (float(final_tax) - df["Tax_percent"])
        / df["Tax_percent"]
    )
    df["Tax_to_pay_RUB"] = abs(df["Tax_to_pay_RUB"])
    df["Country"] = "US"
    df["Amount_Rub"] = df["Amount"] * df["CB_course"]

    return df


def count_dividents_pl_tax(this_year_file, currency_courses_file, finish_tax):

    this_year_df = export_frame_from_csv(this_year_file, "STFU")
    this_year_df["Date"] = pd.to_datetime(this_year_df["Date"], format="%Y%m%d")

    this_year_df = this_year_df[
        this_year_df["ActivityCode"].isin(["PIL", "FRTAX", "DIV"])
    ]
    tax_list = create_div_table(this_year_df)
    pl = create_currency_table_bs(courses_file=currency_courses_file, df=tax_list)
    tax_debt_pl = count_tax_debt(pl, finish_tax)[
        [
            "Symbol",
            "Date",
            "CurrencyPrimary",
            "Amount",
            "Country",
            "Tax",
            "CB_course",
            "Amount_Rub",
            "Tax_RUB",
            "Tax_percent",
            "Tax_to_pay_RUB",
        ]
    ].rename(
        index=str,
        columns={
            "Symbol": "Актив",
            "Country": "Страна",
            "Date": "Дата",
            "CurrencyPrimary": "Валюта",
            "Tax_to_pay_RUB": "Налог к доплате, руб",
            "CB_course": "Курс руб. ЦБРФ",
            "Tax_RUB": "Удержан налог, руб",
            "Tax": "Налог, валюта",
            "Tax_percent": "Размер удержан налога",
            "Amount_Rub": "Сумма выплат, Руб",
            "Amount": "Сумма выплат, Валюта",
        },
    )

    return tax_debt_pl


def main():

    parser = argparse.ArgumentParser(
        description="Export ES query results to Prometheus."
    )

    parser.add_argument(
        "-t", "--tax", default="13", help="Amount of end tax for dividents"
    )

    parser.add_argument(
        "-c",
        "--current-year-file",
        default="example.csv",
        help="Main File with data for analyzing in CSV format",
    )

    parser.add_argument(
        "-p",
        "--previous-year-file",
        action="append",
        default=[],
        help="Helpful file with data for analyzing in CSV format",
    )

    parser.add_argument(
        "-cc",
        "--currency-courses-file",
        default="",
        help="File with currency courses for period of time",
    )

    args = parser.parse_args()

    trn_pl = count_trn_pl(
        args.current_year_file,
        prev_year_file=args.previous_year_file,
        currency_courses_file=args.currency_courses_file,
    )

    div_pl_tax = count_dividents_pl_tax(
        args.current_year_file,
        currency_courses_file=args.currency_courses_file,
        finish_tax=args.tax,
    )
    report_prefix = args.current_year_file.split(".")[0][:-30]
    if not trn_pl.empty:
        pl_to_compare = trn_pl.groupby(["Актив"])[["Прибыль / Убыток, Валюта"]].sum()
        # pl_groupped_by_date = trn_pl.groupby(["Актив"]).sum()
        trn_pl["Дата"] = trn_pl["Дата"].apply(lambda x: x.strftime("%Y-%m-%d"))
        groupped_by_date_pl = (
            trn_pl.replace({"BUY": "Покупка", "SELL": "Продажа"})
            .groupby(
                by=[
                    "Актив",
                    "Op_ID",
                    "Прибыль / Убыток, Валюта",
                    "Прибыль / Убыток, руб",
                    "Дата",
                    "Сделка",
                    "Валюта",
                    # "Стоимость позиции",
                    "Курс руб. ЦБРФ",
                ]
            )[
                [
                    "Кол-во",
                    "Комиссия, Валюта",
                    "Кон.сумма сделки, Валюта",
                    "Кон.сумма сделки, руб",
                ]
            ]
            .sum()
        )
        profit_sum = trn_pl[trn_pl["Кон.сумма сделки, руб"] > 0][
            "Кон.сумма сделки, руб"
        ].sum()
        loss_sum = trn_pl[trn_pl["Кон.сумма сделки, руб"] <= 0][
            "Кон.сумма сделки, руб"
        ].sum()
        groupped_by_date_pl.at["Выручка", "Кон.сумма сделки, руб"] = profit_sum
        groupped_by_date_pl.at["Затраты", "Кон.сумма сделки, руб"] = loss_sum
        # print(groupped_by_date_pl)
        groupped_by_date_pl.reset_index(level="Op_ID", drop=True).to_excel(
            report_prefix + "_PL_groupped.xlsx"
        )
        pl_to_compare.to_excel(report_prefix + "_PL_compare.xlsx")

        trn_pl.to_excel(report_prefix + "_PL.xlsx")
    div_pl_tax["Дата"] = div_pl_tax["Дата"].apply(lambda x: x.strftime("%Y-%m-%d"))

    div_pl_tax.to_excel(report_prefix + "_DIV_TAX.xlsx")


main()
