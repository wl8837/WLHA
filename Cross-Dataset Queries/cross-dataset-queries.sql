-- to find states that have greater than avg number of storm events listing the wind speed, the avg(minimum temperautre), and the avg(maximum temperature)
select main.state, count(second.event_id) as higher_num_storm, round(avg(main.min_temperature),2) as avg_min_temp, round(avg(main.max_temperature),2) as avg_max_temp
from (select s.state_abbreviation, e.event_id from dataset2.storm_events e inner join dataset2.state s on e.state = s.state group by s.state_abbreviation, e.event_id) as second
inner join
(select l.state,t.min_temperature, t.max_temperature from dataset_1.location l inner join dataset_1.temperature t on t.station_id = l.station_id group by l.state,t.min_temperature, t.max_temperature) as main on second.state_abbreviation = main.state
group by main.state, main.min_temperature, main.max_temperature
having higher_num_storm >
(select avg(count) from (select state, count(distinct event_id) as count from dataset2.storm_events group by state))

-- to list states and storm names with the over average number of storm events with average of windspeed
-- to see relationship between windspeed and the type of storm name
select second.state_abbreviation, second.cz_name, count(second.event_id) as higher_num_storm, round(avg(main.wind_speed),2) as avg_windspeed
from (select s.state_abbreviation, e.event_id, e.cz_name from dataset2.storm_events e inner join dataset2.state s on e.state = s.state group by s.state_abbreviation,e.event_id, e.cz_name) as second
inner join
(select l.state, w.wind_speed from dataset_1.location l inner join dataset_1.windspeed w on l.station_id = w.station_id group by l.state, w.wind_speed) as main
on second.state_abbreviation = main.state
where second.state_abbreviation != 'Not US state'
group by second.state_abbreviation, second.cz_name
having higher_num_storm >
(select avg(count) from (select state, count(distinct event_id) as count from dataset2.storm_events group by state))


-- to list states and storm names with the over average number of storm events with average of precipitation
-- to see relationship between precipitation and the type of storm name
select second.state_abbreviation, second.cz_name, count(second.event_id) as higher_num_storm, round(avg(main.precipitation),2) as avg_precipitation
from (select s.state_abbreviation, e.event_id, e.cz_name from dataset2.storm_events e inner join dataset2.state s on e.state = s.state group by s.state_abbreviation, e.event_id, e.cz_name) as second
inner join
(select l.state, p.precipitation from dataset_1.location l inner join dataset_1.Precipitation p on l.station_id = p.station_id group by l.state, p.precipitation) as main
on second.state_abbreviation = main.state
where second.state_abbreviation != 'Not US state'
group by second.state_abbreviation, second.cz_name
having higher_num_storm >
(select avg(count) from (select state, count(distinct event_id) as count from dataset2.storm_events group by state))

-- this query show the relationship between the number of storm and average max and min temperature in each state, each year

select count(secon.event_id) as numb_of_storm, round(avg(main.min_temperature),2) as avg_min, round(avg(main. max_temperature),2) as avg_max, main.state as state, secon.year from (select s.state_abbreviation, e.event_id, EXTRACT(YEAR FROM e.begin_date_time) as year from`clever-guard-230322.dataset2.storm_events` as e
join
`clever-guard-230322.dataset2.state` as s
on s.state = e.state group by s.state_abbreviation, year, e.event_id) as secon
join
(select l.state, EXTRACT(YEAR FROM t.date) as year, min_temperature, max_temperature from `clever-guard-230322.dataset_1.temperature` as t
Join
`clever-guard-230322.dataset_1.location` as l
on l.station_id = t.station_id  group by l.state, year, min_temperature, max_temperature) as main
on main.year = secon.year and secon.state_abbreviation=main.state
group by main.state, secon.year
Order by main.state, secon.year

-- this query show the relationship between the number of storm and average wind speed in each state, each year

select count(secon.event_id) as numb_of_storm, round(avg(main.wind_speed),2) as avg_wind, main.state as state, secon.year from (select s.state_abbreviation, e.event_id, EXTRACT(YEAR FROM e.begin_date_time) as year from`clever-guard-230322.dataset2.storm_events` as e
join
`clever-guard-230322.dataset2.state` as s
on s.state = e.state group by s.state_abbreviation, year, e.event_id) as secon
join
(select l.state, EXTRACT(YEAR FROM t.date) as year, wind_speed from `clever-guard-230322.dataset_1.windspeed` as t
Join
`clever-guard-230322.dataset_1.location` as l
on l.station_id = t.station_id  group by l.state, year, wind_speed) as main
on main.year = secon.year and secon.state_abbreviation=main.state
group by main.state, secon.year

-- this query show the relationship between the number of storm and average precipitation in each state, each year

select count(secon.event_id) as numb_of_storm, round(avg(main.precipitation ),2) as avg_precipitation, main.state as state, secon.year from (select s.state_abbreviation, e.event_id, EXTRACT(YEAR FROM e.begin_date_time) as year from`clever-guard-230322.dataset2.storm_events` as e
join
`clever-guard-230322.dataset2.state` as s
on s.state = e.state group by s.state_abbreviation, year, e.event_id) as secon
join
(select l.state, EXTRACT(YEAR FROM p.date) as year, precipitation from `clever-guard-230322.dataset_1.Precipitation` as p
Join
`clever-guard-230322.dataset_1.location` as l
on l.station_id = p.station_id  group by l.state, year, precipitation) as main
on main.year = secon.year and secon.state_abbreviation=main.state
group by main.state, secon.year
order by main.state, secon.year
