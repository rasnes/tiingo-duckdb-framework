insert or replace into daily_adjusted (date, close, adjClose, adjVolume, ticker)
select date, close, adjClose, adjVolume, ticker
from selected_last_trading_day;
