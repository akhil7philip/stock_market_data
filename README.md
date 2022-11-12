# Stock Market Data Project

This projects retrieves stock market data from an open-source api and stores it in postgres tables

#
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
## import statement for financial data dir
```
sys.path.insert(0,'/home/ubuntu/stock_market_data')
```

#
## approx time taken for each script, in desc order 
* fundamental_tables ~  60 mins each; 18 hours total
* price_volume_tables ~ 60 mins
* general_script_1 ~ 60 mins
* general_script_2 ~ 2 secs

#
## cron commands
* requirement: fundamental - weekly sunday 8 am; price_vol daily 8 am
```
30 06 * * * python3 /home/ubuntu/stock_market_data/financial_data/general_script_1_v2.py > /home/ubuntu/stock_market_data/logs/general_script_1_v2.log  2>&1

30 06 * * * python3 /home/ubuntu/stock_market_data/financial_data/general_script_2_v1.py > /home/ubuntu/stock_market_data/logs/general_script_2_v1.log  2>&1

30 06 * * * python3 /home/ubuntu/stock_market_data/financial_data/price_volume_tables_v2.py > /home/ubuntu/stock_market_data/logs/price_volume_tables_v2.log  2>&1

30 06 * * 0 python3 /home/ubuntu/stock_market_data/financial_data/fundamental_tables.py > /home/ubuntu/stock_market_data/logs/fundamental_tables.log  2>&1
```

#
## Access database
* access database by running below command in terminal:
```
psql postgresql://stock_mkt_user:stock_mkt_pass@localhost:5432/stock_mkt_db
```
* creating a superuser with permission over required database
```
CREATE ROLE stock_mkt_user WITH LOGIN SUPERUSER CREATEDB CREATEROLE PASSWORD 'stock_mkt_pass';
grant all privileges on database stock_mkt_db to stock_mkt_user;
```

#
## script versions:
* general_script_1_v2 processes tables using multiprocessing, unlike simple iteration of each table in general_script_1_v1
* price_volume_tables_1_v2 processes tables using multiprocessing, unlike simple iteration of each table in price_volume_tables_1_v1
* symbols_exchange_v2 returns a list of symbols whose records are not yet saved in the database and then appends the rest of the symbols, unlike symbols_exchange_v1 which passes symbols in the same order every time
* for given 1cpu vm, pool set to 3 workers
