## Why?
This simple python script creates xlsx reports from IB CSV report
## Features
* Creates PL report for transactions
  * Breaks Open/Closed operations to separated pares (even if it wasn't odd)
  * Converts PL report to RUB
* Creates PL report for DIV and FRTAX operations in RUB and count tax to pay

## Params
* `--currency-courses-file` - file with currency courses from USD to RUB in XLSX
* `--current-year-file` - main CSV report from IB
* `--previous-year-file` (Optional) - CSV report from IB for prev years
* `--tax` (Default=13) - tax you have to pay in %

## How to use
```
python3 main.py --currency-courses-file="courses.xlsx"\
                --current-year-file="Account1_2018.txt"\
                --previous-year-file="Account1_2017.txt"\
                --tax=13

```

It will generate files

```
├── Account1_2018_DIV_TAX.xlsx
├── Account1_2018_PL.xlsx
├── Account1_2018_PL_compare.xlsx

```
