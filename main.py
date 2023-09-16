import logging
from settings import *
from financial_data import *


# price_volume_tables_v3.main_func()


args = [
        # (endpoint, period, limit, table_name)
        ('ratios-ttm', 'annual', 30, 'annual_ratios_ttm'),
        ('key-metrics-ttm', 'annual', 30, 'annual_key_metrics_ttm'),
        ('income-statement', 'annual', 30, 'annual_income_statement'),
        ('income-statement', 'quarter', 120, 'quarter_income_statement'),
        ('income-statement-growth', 'annual', 30, 'annual_income_statement_growth'),
        ('income-statement-growth', 'quarter', 120, 'quarter_income_statement_growth'),
        ('balance-sheet-statement', 'annual', 30, 'annual_balance_sheet_statement'),
        ('balance-sheet-statement', 'quarter', 120, 'quarter_balance_sheet_statement'),
        ('balance-sheet-statement-growth', 'annual', 30, 'annual_balance_sheet_statement_growth'),
        ('balance-sheet-statement-growth', 'quarter', 120, 'quarter_balance_sheet_statement_growth'),
        ('cash-flow-statement', 'annual', 30, 'annual_cash_flow_statement'),
        ('cash-flow-statement', 'quarter', 120, 'quarter_cash_flow_statement'),
        ('cash-flow-statement-growth', 'annual', 30, 'annual_cash_flow_statement_growth'),
        ('cash-flow-statement-growth', 'quarter', 120, 'quarter_cash_flow_statement_growth'),
        ('ratios', 'annual', 30, 'annual_ratios'),
        ('ratios', 'quarter', 120, 'quarter_ratios'),
        ('key-metrics', 'annual', 30, 'annual_key_metrics'),
        ('key-metrics', 'quarter', 120, 'quarter_key_metrics')
    ]
fundamental_tables.mp_main(args)