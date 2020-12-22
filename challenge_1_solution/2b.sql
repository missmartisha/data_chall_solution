-- (PIT) 30day moving average inventory balance --
select item_id,
location_id,
avg(item_balance) over (partition by item_id, location_id 
order by dt asc
rows between 29 preceding and current row) as aver_30days_balance
from invent_balance;
