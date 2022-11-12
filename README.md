# Stock Market Data Project

This projects retrieves stock market data from an open-source api and stores it in postgres tables

## How to run the project:
* create a conda environment and activate it
```
conda create -n stock_mkt_data python=3.10
conda activate stock_mkt_data
```
* install requirements
```
pip install -r requirements.txt
```
* run the individual files in fundamental tables dir
```
python fundamental_tables.py
python general_script_1_v2.py
python price_volume_tables_v2.py
```
#
## script versions:
* general_script_1_v2 processes tables using multiprocessing, unlike simple iteration of each table in general_script_1_v1
* price_volume_tables_1_v2 processes tables using multiprocessing, unlike simple iteration of each table in price_volume_tables_1_v1
* symbols_exchange_v2 returns a list of symbols whose records are not yet saved in the database and then appends the rest of the symbols, unlike symbols_exchange_v1 which passes symbols in the same order every time
