create extension hstore;

create table location (
  id serial primary key,
  parent int references location,

  name text not null,
  sms_code varchar(10) unique,
  type text not null,

  attr hstore,
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
--   select agg, ...
--   from by_ancestor('someloc', array(select id from location where parent = 'someloc')) join
--        location l on (loc.id = l.id)
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
  name text not null,
  --units, price, category, description, etc.
)

create table stocktx (
  id int primary key,
  at_ timestamp not null,
  --submission text, (will link to submitting xform? (for metadata like user, xform id, etc)
  location references location not null,
  product references product not null,
  action_ varchar(20) not null,
  subaction varchar(20),
  quantity float8
)
create index on stocktx(at_);

create table snapshot (
  location
  product
  current_stock
  stocked_out_since
  last_reported_on
  consumption_rate
)

create view xxx as -- stock/consumption

-- hist_snapshot?
