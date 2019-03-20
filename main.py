from datetime import datetime, date, time
from copy import deepcopy
import pandas as pd
from typing import NamedTuple
import sys
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
            if start_frame in line:
                frames.append(dict(
                    name=line.strip().split("|")[2],
                    label=line.strip().split("|")[1],
                    bos_line=num,
                    eos_line=0
                ))
            if end_frame in line:
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
    if len(table1[table1["Open/CloseIndicator"].isin(["O", "C"])].index) == 0 or len(
            table2[table2["Open/CloseIndicator"].isin(["O", "C"])].index) == 0:
        return result_table, table1, table2
    else:
        table1, table2 = table2, table1
        table1_copy = table1.copy(deep=True)
        for row1 in table1_copy.iterrows():
            if row1[1]["Open/CloseIndicator"] != "O" and row1[1]["Open/CloseIndicator"] != "C":
                continue
            row2_o = table2[(table2.Symbol == row1[1]["Symbol"]) & (
                table2["Buy/Sell"] != row1[1]["Buy/Sell"])]
            if row2_o.empty:
                row1[1]["Open/CloseIndicator"] += ";P"
                table1.loc[row1[0]] = row1[1]
            else:
                row2_o = row2_o[(abs(row2_o["Quantity"]) <=
                                 abs(row1[1]["Quantity"]))]
                if row2_o.empty:
                    continue
                row2 = row2_o.loc[row2_o.index[0]]

                copy_row1 = deepcopy(row1)
                result_table = result_table.append(row2)

                oc_ref = abs(row2["Quantity"] / row1[1]["Quantity"])

                row1[1]["Quantity"] = -1 * row2["Quantity"]
                row1[1]["Proceeds"] = oc_ref * row1[1]["Proceeds"]
                row1[1]["IBCommission"] = oc_ref * row1[1]["IBCommission"]

                pl = row1[1]["Proceeds"] + row2["Proceeds"] + \
                    row1[1]["IBCommission"] + row2["IBCommission"]

                result_table = result_table.append(row1[1])
                result_table.at[row1[0], "PL"] = pl
                table2 = table2.drop(row2_o.index[0])
                copy_row1[1]["Quantity"] -= row1[1]["Quantity"]
                copy_row1[1]["Proceeds"] -= row1[1]["Proceeds"]
                copy_row1[1]["IBCommission"] -= row1[1]["IBCommission"]

                if copy_row1[1]["Quantity"] == 0:
                    table1 = table1.drop(copy_row1[0])
                else:
                    table1.at[copy_row1[0],
                              "Quantity"] = copy_row1[1]["Quantity"]
                    table1.at[copy_row1[0],
                              "Proceeds"] = copy_row1[1]["Proceeds"]

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
            "PL"])
    return merge_tables(open_close_df, open_operations, close_operations)


def create_pl_table(pd_frames):

    open_operations = pd_frames[pd_frames["Open/CloseIndicator"].isin(["O"])]
    close_operations = pd_frames[(
        pd_frames["Open/CloseIndicator"].isin(["C"]))]

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
            "PL"])
    oc_df, df_1, df_2 = merge_tables(
        open_close_df, open_operations, close_operations)

    return oc_df, df_1, df_2


def setter(x, key, val):
    x[[key]] = val
    return x


def create_currency_table_bs(courses_file, df):
    currency_table = pd.read_excel(courses_file).rename(
        index=str,
        columns={
            "curs": "CB_course",
            "data": "Date"}).filter(
        items=[
            "Date",
            "CB_course"])

    pl_table = pd.merge(df, currency_table, on="Date", sort=False, how="left")

    pl_table[["CB_course", "Date"]] = pl_table[["CB_course", "Date"]].apply(lambda x: x if x["CB_course"] > 0 else setter(
        x, "CB_course", currency_table.iloc[[(currency_table.Date[currency_table.Date <= x["Date"]] - x["Date"]).idxmax()]]["CB_course"]), axis=1)

    pl_table["PL_Rub"] = pl_table.PL * pl_table.CB_course

    return pl_table


def export_frame_from_csv(csv_file, frame_name):

    frame_items = {
        "TRNT": [
            "Symbol",
            "TradeDate",
            "Buy/Sell",
            "CurrencyPrimary",
            "Quantity",
            "TradePrice",
            "IBCommission",
            "Proceeds",
            "Open/CloseIndicator"],

        "STFU": [
            "Symbol",
            "ActivityCode",
            "Date",
            "CurrencyPrimary",
            "Amount"
        ]}

    frames = find_frames_in_csv(csv_file)

    return export_from_csv(
        csv_file, search(
            frame_name, frames)).filter(
        items=frame_items[frame_name])


def count_trn_pl(this_year_file, currency_courses_file, prev_year_file=""):

    this_year_df = export_frame_from_csv(
        this_year_file, "TRNT").rename(
        index=str, columns={
            "TradeDate": "Date"})

    this_year_df["Date"] = pd.to_datetime(
        this_year_df["Date"], format="%Y%m%d")
    this_year_df = this_year_df[(abs(this_year_df["Proceeds"])) > 0]

    this_year_pl, df_1, df_2 = create_pl_table(this_year_df)
    prev_year_pl = pd.DataFrame()
    # Solve for prev year with orphaned closed positions from first year

    if prev_year_file != "":
        prev_year_df = export_frame_from_csv(
            prev_year_file, "TRNT").rename(
            index=str, columns={
                "TradeDate": "Date"})
        prev_year_df["Date"] = pd.to_datetime(
            prev_year_df["Date"], format="%Y%m%d")
        prev_year_df = prev_year_df[prev_year_df["Open/CloseIndicator"] == "O"]

        additional_trans_df = pd.concat(
            [
                prev_year_df[prev_year_df["Open/CloseIndicator"] == "O"],
                df_1[df_1["Open/CloseIndicator"] == "C;P"],
                df_2[df_2["Open/CloseIndicator"] == "C;P"]
            ]).reset_index(drop=True)
        additional_trans_df.loc[additional_trans_df["Open/CloseIndicator"]
                                == "C;P", "Open/CloseIndicator"] = "C"

        prev_year_pl, df_1, df_2 = create_pl_table(additional_trans_df)

    pl_df = pd.concat([prev_year_pl, this_year_pl])
    pl_df["Cash"] = pl_df["IBCommission"] + pl_df["Proceeds"]
    pl = create_currency_table_bs(courses_file=currency_courses_file, df=pl_df)

    finish_pl = pl.rename(index=str, columns={
        "Symbol": "Актив",
        "Date": "Дата",
        "CurrencyPrimary": "Валюта",
        "Buy/Sell": "Сделка",
        "Quantity": "Кол-во",
        "TradePrice": "Стоимость позиции",
        "CB_course": "Курс руб. ЦБРФ",
        "PL_Rub": "Прибыль / Убыток, руб.",
        "PL": "Прибыль / Убыток, Валюта",
        "Cash": "Кон.сумма сделки, Валюта"})

    return finish_pl


def create_div_table(df):
    divs_table = pd.DataFrame(columns=[
        "Symbol",
        "Date",
        "CurrencyPrimary",
        "Amount",
        "Tax",
        "PL"
    ])

    for row in df[df["ActivityCode"] == "DIV"].iterrows():
        div_row = deepcopy(row)
        find_tax = df[(df["ActivityCode"] == "FRTAX") & (
            df["Symbol"] == row[1]["Symbol"]) & (df["Date"] == row[1]["Date"])]
        div_row[1]["Tax"] = find_tax["Amount"].sum()

        divs_table = divs_table.append(div_row[1])
    divs_table["PL"] = divs_table["Amount"] + divs_table["Tax"]

    return divs_table


def count_tax_debt(df, final_tax):

    df["tax_percent"] = abs(df["Tax"] / df["Amount"]) * 100
    df["tax_RUB"] = df["Tax"] * df["CB_course"]

    df["tax_to_pay_RUB"] = df[df["tax_percent"] <= float(
        final_tax)]["tax_RUB"] * (float(final_tax) - df["tax_percent"]) / df["tax_percent"]

    return df


def count_dividents_pl_tax(this_year_file, currency_courses_file, finish_tax):

    this_year_df = export_frame_from_csv(
        this_year_file, "STFU")
    this_year_df["Date"] = pd.to_datetime(
        this_year_df["Date"], format="%Y%m%d")

    this_year_df = this_year_df[this_year_df["ActivityCode"].isin([
                                                                  "FRTAX", "DIV"])]
    tax_list = create_div_table(this_year_df)
    pl = create_currency_table_bs(
        courses_file=currency_courses_file, df=tax_list)
    tax_debt_pl = count_tax_debt(pl, finish_tax)

    return tax_debt_pl


def main():

    finish_tax = "13"
    currency_courses_file = "RC_F09_03_2017_T16_03_2019 (1).xlsx"

    prev_year_file = "IB_2017.txt"
    this_year_file = "report_2.txt"

    trn_pl = count_trn_pl(
        this_year_file,
        prev_year_file=prev_year_file,
        currency_courses_file=currency_courses_file)

    div_pl_tax = count_dividents_pl_tax(
        this_year_file,
        currency_courses_file=currency_courses_file, finish_tax=finish_tax)

    trn_pl.to_excel("trn_pl.xlsx")
    trn_pl.to_excel("div_pl_tax.xlsx")

    print(trn_pl)


main()
