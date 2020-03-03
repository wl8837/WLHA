
create table dataset_1.temperature as
select mx.serialid, mx.station_id, cast(mx.date as date) as date, min_temperature, max_temperature
from (
select serialid, id as station_id, date, value as max_temperature
from dataset_1.maximum_temperatures
where serialid is not null) as mx
full outer join
(
select serialid, id as station_id, date, value as min_temperature
from dataset_1.minimum_temperatures
where serialid is not null
) as mn on mx.serialid = mn.serialid
order by serialid asc


create table dataset_1.Precipitation as
select serialid, id as station_id, cast (date as date) as date, value as precipitation
from dataset_1.precipitation
where serialid is not null


create table dataset_1.snow as
select sf.serialid, sf.station_id, cast (sf.date as date) as date, snow_depth, snowfall
from
(
select serialid, id as station_id, date, value as snow_depth
from dataset_1.snowdepth
where serialid is not null
) as sd
full outer join
(select serialid, id as station_id, date, value as snowfall
from dataset_1.snowfall
where serialid is not null) as sf
on sd.serialid = sf.serialid
order by serialid asc


create table dataset_1.windspeed as
select serialid, id as station_id, cast(date as date) as date, value as wind_speed
from dataset_1.wind_speed
where serialid is not null
order by serialid asc


create table dataset_1.date as
select cast(date as date) as date
from dataset_1.maximum_temperatures as max
union distinct
select cast(date as date) as date
from dataset_1.minimum_temperatures as min
union distinct
select cast(date as date) as date
from dataset_1.precipitation
union distinct
select cast(date as date) as date
from dataset_1.snowdepth
union distinct
select cast(date as date) as date
from dataset_1.snowfall
union distinct
select cast(date as date) as date
from dataset_1.wind_speed
order by date


create table dataset_1.location as
select  id as station_id, name as station_name, latitude, longitude  state
from dataset_1.maximum_temperatures
union distinct
select id as station_id, name as station_name, latitude, longitude  state
from dataset_1.minimum_temperatures
union distinct
select id as station_id, name as station_name, latitude, longitude  state
from dataset_1.precipitation
union distinct
select id as station_id, name as station_name, latitude, longitude  state
from dataset_1.snowdepth
union distinct
select id as station_id, name as station_name, latitude, longitude  state
from dataset_1.snowfall
union distinct
select id as station_id, name as station_name, latitude, longitude  state
from dataset_1.wind_speed
order by station_id
