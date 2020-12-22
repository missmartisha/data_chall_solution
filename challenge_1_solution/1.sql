------- (1) if transaction is uniquely identified by item id, location id, transaction date  (that is the case in the provided source data set) -------

-- PIT inventory balance --
create table if not exists invent_balance as
with dim_item_loc as (
select item_id, 
location_id 
from invent_movement 
group by 1,2
),
dw as (
select 
il.item_id,
il.location_id, 
date(generate_series((select min(trans_date) from invent_movement), current_date, '1 day'::interval)) as dt
from dim_item_loc il
)
select 
item_id,
location_id,
dt,
sum(daily_trans_quantity) over (partition by item_id, location_id order by dt asc rows between unbounded preceding and current row) as item_balance
from (
select dw.item_id,
dw.location_id,
dw.dt,
coalesce(i.trans_quantity, 0) as daily_trans_quantity
from dw
left join invent_movement i on dw.item_id = i.item_id and dw.location_id = i.location_id and dw.dt = i.trans_date
) daily_movement;

------- (2) if transaction is  not uniquely identified by item id, location id, transaction date  (not the case in the provided source data set but usually is the case for item movement fact tables) -------

-- PIT inventory balance --
create table if not exists invent_balance as
with dim_item_loc as (
select item_id, 
location_id 
from invent_movement 
group by 1,2
),
dw as (
select 
il.item_id,
il.location_id, 
date(generate_series((select min(trans_date) from invent_movement), current_date, '1 day'::interval)) as dt
from dim_item_loc il
)
select 
item_id,
location_id,
dt,
sum(daily_trans_quantity) over (partition by item_id, location_id order by dt asc rows between unbounded preceding and current row) as item_balance
from (
select dw.item_id,
dw.location_id,
dw.dt,
sum(coalesce(i.trans_quantity, 0)) as daily_trans_quantity
from dw
left join invent_movement i on dw.item_id = i.item_id and dw.location_id = i.location_id and dw.dt = i.trans_date
group by 1,2,3
) daily_movement;