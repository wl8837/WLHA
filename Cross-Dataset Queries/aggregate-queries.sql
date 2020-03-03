--this query is to look at minimum, maximum, and average of max temperature in temperature table.
Select station_id, min(max_temperature), max(max_temperature), avg(max_temperature)
from dataset_1.temperature
Group by station_id
--data are organized by station_id
order by station_id


--this query is to look at minimum, maximum, and average of max temperature in temperature table. By
Select EXTRACT(YEAR FROM date) as year, min(max_temperature), max(max_temperature), avg(max_temperature)
from dataset_1.temperature
Group by year
--data are organized by year
having year>2008
-- only select the data after 2008
order by year

--this query is to look at minimum, maximum, and average of snowfall in snow table. By
Select EXTRACT(YEAR FROM date) AS year, min(snowfall), max(snowfall), avg(snowfall)
from dataset_1.snow
Group by year
--data are organized by year
having year>2008
-- only select the data after 2008
order by year


--this query is to look at minimum, maximum, and average of snow depth in snow table. By
Select EXTRACT(YEAR FROM date) AS year, min(snow_depth), max(snow_depth), avg(snow_depth)
from dataset_1.snow
Group by year
--data are organized by year
having year>2008
-- only select the data after 2008
order by year


-- For each year, what is the average windspeed
select EXTRACT(YEAR FROM date) as year, round(avg(wind_speed),2) as avg_windspeed
from dataset_1.windspeed
group by year
having year > 2008
order by year


-- For each state, what is the average of min_temperture and max_temperature for the state
select l.state, round(avg(t.min_temperature),2) as avg_min_temp, round(avg(max_temperature),2) as avg_max_temp
from dataset_1.location l right join dataset_1.temperature t on l.station_id = t.station_id
where l.state is not null
group by l.state
order by l.state


-- For each state, the average of snowfall and snow_depth
select l.state, round(avg(s.snowfall),2) as avg_snowfall, round(avg(s.snow_depth),2) as avg_snowdepth
from dataset_1.location l right join dataset_1.snow s on l.station_id = s.station_id
where l.state is not null
group by l.state
order by l.state


-- for each year, count the number of precipitation is greater than 0
select EXTRACT(YEAR FROM date) as year, count(p.precipitation) as num_precipitation
from dataset_1.Precipitation p
where p.precipitation > 0
group by year
having year > 2008
order by year
