-- select "value (=max. temp)" field from Maximum_temperatures table
SELECT mx.value FROM dataset_1.maximum_temperatures mx
-- where max. temp is not null in ascending order
WHERE NOT(mx.value IS NULL) ORDER BY mx.value ASC

-- select "value (=max. temp)" field from Maximum_temperatures table
SELECT mx.state FROM dataset_1.maximum_temperatures mx
-- where max. temp is not null and list state in ascending order
WHERE NOT(mx.value IS NULL) ORDER BY mx.state ASC

-- select "value (=min.temp)" field from Minimum_temperature table
SELECT mn.value FROM dataset_1.minimum_temperatures mn
-- where min.temp is not null in descending order
WHERE NOT(mn.value IS NULL) ORDER BY mn.value DESC

-- select "value (=min.temp)" field from Minimum_temperature table
SELECT mn.date FROM dataset_1.minimum_temperatures mn
-- where min.temp is not null in descending order
WHERE NOT(mn.value IS NULL) ORDER BY mn.date ASC

-- select "value (=precipitation)" field from precipitation table
SELECT p.value FROM dataset_1.precipitation p
-- where precipitation is greater than 0 in ascending order
WHERE p.value > 0  ORDER BY p.value ASC

-- select "value (=snowdepth)" field from snowdepth table
SELECT sd.value FROM dataset_1.snowdepth sd
-- where snowdepth is greater than 0 in descending order
WHERE sd.value > 0  ORDER BY sd.value DESC

-- select "value (=snowfall)" field from snowfall table
SELECT sf.value FROM dataset_1.snowfall sf
-- where snowfall is greater than 10 mm in ascending order
WHERE sf.value >10 ORDER BY sf.value ASC

-- select "value (=average wind speed)" field from wind_speed table
SELECT w.value FROM dataset_1.wind_speed w
-- where value is greater than 5 in descending order
WHERE w.value > 5 ORDER BY w.value DESC

-- select "state" field from wind_speed table
SELECT w.state FROM dataset_1.wind_speed w
-- where value is greater than 5 and list state in ascending order
WHERE w.value > 5 ORDER BY w.state ASC
