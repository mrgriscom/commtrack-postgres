--create extension hstore; -- requires superuser; disable for now

create table location (
  id serial primary key,
  parent int references location,

  name text not null,
  sms_code varchar(10) unique,
  type text not null,

  --attr hstore,
  geo text --WKT format compatible with postgis
);
create index on location(parent);

-- retrieve all descendants of a location
-- arg1: root ancestor loc_id (can't be null, so use an explicit root location)
-- returns: all descendants, one per row;
--    each row: loc_id of descendant, array of lineage -- all ancestor locs up
--       to and including the root loc
create or replace function descendants(int) 
returns table(id int, lineage int[])
as $$
  with recursive descendants as (
    select id, array[]::int[] as lineage from location where id = $1
    union
    select l.id, lineage || l.parent from location l, descendants where l.parent = descendants.id
  )
  select * from descendants where id != $1;
$$ language sql;

-- match up locations to an ancestor loc for aggregation
-- arg1: root ancestor loc of all locations to search
-- arg2: array of ancestors loc_ids used for aggregation, e.g,
--    array(select name from location where parent = '...')
-- returns a mapping of descendant locs to the aggregating loc in arg2 that
--    appears in its lineage (suitable for passing to a group-by sql statement)
-- example:
--   select agg, <aggregate exprs>
--   from by_ancestor('someloc', array(select id from location where parent = 'someloc')) join
--        location l on (loc = l.id)
--   group by agg
create or replace function by_ancestor(int, int[])
returns table(agg int, loc int)
as $$
  select agg, locs.id as loc
  from (select * from descendants($1)) locs
    join unnest($2) as agg
    on (agg = any(locs.lineage || locs.id));
$$ language sql;



create table product (
  sms_code varchar(10) primary key,
  name text not null
  --units, price, category, description, etc.
);

create table stocktransaction (
  id serial primary key,
  at_ timestamp not null,
  submission text, --(will link to uuid submitting xform? (for metadata like user, xform id, etc))
  location int references location not null,
  product varchar(10) references product not null,
  action_ varchar(20) not null,
  subaction varchar(20),
  quantity float8
);
-- this index is necessary for computing consumption
create index on stocktransaction(location, product, at_);
-- this index is for querying stock activity history
create index on stocktransaction(at_, location);

-- return the relevant stock transactions for computing consumption rate
-- arg1: location id
-- arg2: product id
-- arg3: start of window
-- arg4: end of window
-- arg5: list of action types that allow us to determine actual count of stock (as
--    opposed to a diff from a previous count)
create or replace function consumption_transactions(int, text, timestamp, timestamp, text[])
returns setof stocktransaction
as $$
  select * from stocktransaction
  where (location, product) = ($1, $2) and at_ between coalesce((
      -- get the date of the most recent 'full-count' stock transaction before the window start
      select max(at_) from stocktransaction 
      where (location, product) = ($1, $2) and at_ < $3
        and action_ = any($5)
    ), $3) and $4
  order by at_, id  -- id is importart to preserve intra-stockreport ordering
$$ language sql;




create table stockstate (
  id serial primary key,

  at_ timestamp not null,
  location int references location not null,
  product varchar(10) references product not null,
  
  current_stock float8 not null,
  stock_out_since timestamp,
  last_reported timestamp, -- tracks date of last soh report
  consumption_rate float8
);
-- this index is necessary for finding the state at a given point in time (or the present)
create index on stockstate(location, product, at_ desc);


-- shows the current stock for all locations/products
create view current_state as
select distinct on (location, product)
  *, current_stock / nullif(consumption_rate, 0) as months_remaining
from stockstate
order by location, product, at_ desc;
-- note: filter by a location subset, for small location sets i seem to get better
-- performance using 'where location = any(array(<locs>))', but at a certain size
-- it seems more efficient to do a subquery ('where location in (...)') or a join



-- last soh received for ANY product for each location
create view last_reported as
select location, max(last_reported) as last_reported
from current_state
group by location;


-- REPORTS --

-- arg1: location id
create or replace function aggregate_stock_report(int)
returns table(product text, stock float8, consumption float8, month_remaining float8)
as $$
  select product, sum(current_stock), sum(consumption_rate),
    sum(consumable_stock) / nullif(sum(consumption_rate), 0) as months_remaining
  from (
    --exclude stock with no corresponding consumption
    select *, current_stock + 0 * consumption_rate as consumable_stock
    from current_state
  ) x
  where location in (select id from descendants($1))
  group by product;
$$ language sql;
-- this seems to get inefficient for very large location sets... i think because it
-- tries to evaluate the subquery multiple times in an attempt to be smarter. we could
-- enforce only evaluating it once by moving it to a separate function, i think

-- arg1: location id
-- arg2: date threshold for lateness
-- arg3: date threshold for non-reporting
create or replace function reporting_status_report(int, timestamp, timestamp)
returns table(location int, num_sites bigint, pct_ontime float8, pct_late float8, pct_nonrep float8)
as $$
  select agg, count(*) as num_sites,
    count(ontime)::float8 / count(*) as pct_ontime,
    count(late)::float8 / count(*) as pct_late,
    count(nonrep)::float8 / count(*) as pct_nonrep
  from (
    select *,
      nullif(last_reported > $2, false) as ontime,
      nullif(last_reported between $3 and $2, false) as late,
      nullif(last_reported < $3, false) as nonrep
    from by_ancestor($1, array(select id from location where parent = $1)) agg
      join last_reported lr on (agg.loc = lr.location)
  ) x
  group by agg;
$$ language sql;

-- arg1: months of stock remaining
-- todo: access location for configurable thresholds; maybe call back out to python?
create or replace function stock_status(float8) returns text as $$
select case
  when $1 is null then 'nodata'
  when $1 = 0 then 'stockout'
  when $1 < .5 then 'low'
  when $1 > 2. then 'over'
  else 'adequate'
  end;
$$ language sql;

--arg1: location id
create or replace function aggregate_stock_status_by_product_report(int)
returns table(product text, num_sites bigint, stock_status text, pct float8)
as $$
  with entries as (
    select location, product, stock_status(months_remaining)
    from current_state join descendants($1) d on (current_state.location = d.id)
  )
  select a.product, num_sites, stock_status, subtally::float8 / num_sites as pct
  from (
    select product, stock_status, count(*) as subtally from entries
    group by product, stock_status
  ) a join (
    select product, count(*) as num_sites from entries 
    group by product
  ) b on (a.product = b.product);
$$ language sql;

