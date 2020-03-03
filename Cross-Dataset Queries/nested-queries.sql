--this query shows station id  average with minimum temperature higher --than overall average minimum temperature
select station_id, AVG(min_temperature) as high_min_temperature
from dataset_1.temperature
Where min_temperature > (select AVG(min_temperature) as avg_min_temp
from dataset_1.temperature)
group by station_id


--this query shows station id  average with maximum temperature higher --than overall average maximum temperature

select station_id, AVG(max_temperature) as high_max_temperature
from dataset_1.temperature
Where max_temperature > (select AVG(max_temperature) as avg_max_temp
from dataset_1.temperature)
group by station_id


-- this query shows station id with average snowfall higher than overall -- average and its precipitation 

Select snow.station_id, snow.date, AVG(snowfall) as high_snowfall, AVG(precipitation) as ave_precipitation
from dataset_1.snow as snow
JOIN dataset_1.Precipitation as pre
ON snow.station_id = pre.station_id and snow.date=pre.date
where snowfall > (select ave_snowfall
from
(Select AVG(snowfall) as ave_snowfall
from dataset_1.snow as snow
))
group by snow.station_id, snow.date
order by snow.station_id

--this query shows station ids with average snowfall higher than overall -- average  and its average temperature.


Select wind.station_id, wind.date, AVG(wind_speed) as high_wind, AVG(min_temperature) as ave_min_temperature, AVG(min_temperature) as ave_max_temperature
from dataset_1.windspeed as wind
JOIN dataset_1.temperature as tem
ON wind.station_id = tem.station_id and wind.date=tem.date
group by wind.station_id, wind.date 
where wind_speed> (select avg_wind
from
(select AVG(wind_speed) as avg_wind
from dataset_1.windspeed as wind
))
 
 
-- states that have a lower min_temperture than the overall average of min_temperture
select l.state, t.min_temperature, round((select avg(min_temperature) from dataset_1.temperature),2) as avg_min_temp
from dataset_1.location l right join dataset_1.temperature t on l.station_id = t.station_id
where l.state is not null and t.min_temperature < (select avg(min_temperature) from dataset_1.temperature)
group by l.state, t.min_temperature

-- state that have a higher max_temperature than the overall average of max_temperature
select l.state, t.max_temperature, round((select avg(max_temperature) from dataset_1.temperature),2) as avg_max_temp
from dataset_1.location l right join dataset_1.temperature t on l.station_id = t.station_id
where l.state is not null and t.max_temperature < (select avg(max_temperature) from dataset_1.temperature)
group by l.state, t.max_temperature

-- state and windspeed that have wind_speed is greater than 15 tenth meter per second and snowfall is greater than 10 
select state, wind_speed
from dataset_1.windspeed w left join dataset_1.location l on w.station_id = l.station_id
where wind_speed in
(select wind_speed w from dataset_1.windspeed w left join dataset_1.snow s on w.station_id = s.station_id
where w.wind_speed > 15 and s.snowfall > 10)
group by state, wind_speed

-- snow_depth and snowfall that are greater than avg of snow_depth that is greater than 0 and snowfall that are greater than avg of snowfall that is greater than 0
select snow_depth, round(Select avg(snow_depth) from dataset_1.snow where snow_depth > 0),2) as avg_depth, snowfall, round((select avg(snowfall) from dataset_1.snow where snowfall > 0),2) as avg_fall
from dataset_1.snow
where snow_depth > (Select avg(snow_depth) from dataset_1.snow where snow_depth > 0) and snowfall > (select avg(snowfall) from dataset_1.snow where snowfall > 0)
