select ticker
from selected_last_trading_day
where splitFactor != 1.0 or divCash > 0;
