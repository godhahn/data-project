create or replace view weather_base_vw as
select 
    weather_data.date,
    datatype_data.name as datatype,
    station_data.name as station,
    weather_data.value
from weather_data
left outer join datatype_data
on weather_data.datatype = datatype_data.id
left outer join station_data
on weather_data.station = station_data.id
order by date desc