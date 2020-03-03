WLHA\
Wenkang Li\
liwhenkang@gmail.com\
Hyejin An\
hyejin.an@utexas.edu
source: National Climatic Weather Center

#### Main dataset
Daily Average Wind Speed
Daily Maximum Temperatures
Daily Minimum Temperatures
Daily Precipitation
Daily Snow Depth
Daily Snowfall

# Link
https://public.enigma.com/browse/collection/national-climatic-data-center/b87242f0-65e0-4f9f-a051-084d63ca6dd9

# Description
from 2008 to 2013
Daily Average Wind Speed: Daily average wind speed for global weather stations in tenths of a meter per second 
Daily Maximum Temperatures: Daily maximum temperatures in tenths of a degree celsius for global weather stations
Daily Minimum Temperatures: Daily minimum temperatures in tenths of a degree celsius for global weather stations 
Daily Precipitation: Daily precipitation in millimeters for global weather stations
Daily Snow Depth: Daily snow depth in millimeters for global weather stations
Daily Snowfall: Daily snowfall in millimeters for global weather stations

# Interesting attributes and relationships
- interesting attributes
StationID  Date  State  Value of Indicator  MaxTemp  AvgWindSpeed  MinTemp  Precipitation (mm) SnowDepth (mm)  
Snowfall (mm) 

- relationships
Relationship between Temperatures and Snow
Relationship between Temperature and Snow
Relationship between Wind and Snow

# Insight we hope to see (possible things)
[1] Hope to see if there is a big relationship between Wind, Temperature, and Snow
[2] Hope to see How we can predict snow or weather condition using those intersting attributes
[3] Hope to see if there is any weather change in specific locations by years 

#### Second dataset 
Source from "National Climatic Data Center" in Public Enigma
We will join second dataset on main dataset with state attribute

# Link
https://public.enigma.com/browse/collection/national-climatic-weather-center-storm-events/3d0d8bdf-b885-48b9-a3b1-8a8441fc0131

# Description
Storm data (2009-2013) that is an official publication of the National Oceanic and Atmospheric Administration (NOAA) 
which documents:
[1] The occurrence of storms and other significant weather phenomena having sufficient intensity to cause loss of life, injuries, significant property damage, and/or disruption to commerce; 
[2] Rare, unusual, weather phenomena that generate media attention, such as snow flurries in South Florida or the San Diego coastal area; and 
[3] Other significant meteorological events, such as record maximum or minimum temperatures or precipitation that occur in connection with another event.

# Interesting attributes and relationships
- Interesting attributes
Begin Year/Month, Begin Day, Begin Time, End Year/Month, End Day, End Time, EventID, state, Storm / Event Name

- relationships
relationship between date and strom event
relationship between state and storm event

# Insight we hope to see with Main dataset
Hope to see any relationship between weather factors such as attributes in Main dataset (which are temperature, precipitation,
wind speed, snow) and the number of storm events in the U.S. 
