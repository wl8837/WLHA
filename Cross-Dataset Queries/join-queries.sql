-- finding minimum_temperatures from minimum_temperatures table
select mn.value from dataset_1.minimum_temperatures mn
-- by joining snowdepth table with same id in each table
join dataset_1.snowdepth sd on mn.id = sd.id
-- where the weather condition happened in same dates
where mn.date = sd.date


-- finding date, state, and average wind spped from wind speed table
select w.date, w.state, w.value from dataset_1.wind_speed w
-- by left joing snowfall table with same id each
left join dataset_1.snowfall sf on w.id = sf.id
-- where wind speed is not zero
where not(w.value = 0)


-- finding precipitation value from precipitation table
select p.value from dataset_1.precipitation p
-- by right joining on wind_speed table with date and id
right join dataset_1.wind_speed ws on p.date = ws.date and p.id = ws.id
-- order by precipitation in ascending
order by p.value ASC


SELECT wind_speed.date AS wd, wind_speed.value AS wv, wind_speed.name AS wn FROM dataset_1.wind_speed
--select data, value, and name column from wind_speed table. In order to avoid ambiguous column name, I mark date, value, name column as wd, wv, wn
LEFT JOIN dataset_1.maximum_temperatures ON wind_speed.date = maximum_temperatures.date and wind_speed.name=maximum_temperatures.name
--Using left join method(in this case, keeping all items in the wind_speed table), connecting wind_speed table and  maximum_temperatures table, when date and name column in maximum_temperatures table matches date and name in wind_speed table.


SELECT w.value AS wv, w.date AS wd, w.name AS wn  FROM dataset_1.wind_speed AS w
--select data, value, and name column from wind_speed table (referred here as w). In order to avoid ambiguous column name, I mark date, value, name columns as wd, wv, wn.
INNER JOIN dataset_1.snowfall AS s ON w.name=s.name and w.date=s.date
--Using inner join method to connect snowfall table (referred here as w) when name, and date column in snowfall table matches name, and date column in wind_speed table.


select max.date as maxd, max.value as maxv, max.name as maxn FROM dataset_1.maximum_temperatures as max
--select data, value, and name column from maximum_temperatures table(referred as “max”). In order to avoid ambiguous column name, I mark date, value, name columns as maxd, maxv, maxn.
FULL OUTER JOIN dataset_1.minimum_temperatures as min ON min.date = max.date and min.name = max.name
--using full other join(keeping items from both tables). Matching date, and name column in maximum_temperatures to date, and name column in minimum_temperatures table.
