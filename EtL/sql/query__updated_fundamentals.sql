select ticker from fundamentals.selected_fundamentals
where {{.DateColumn}} >= current_date - interval '{{.DaysBackfill}} days'
