create or replace view weather_base_vw as
select distinct
    date,
    station,
    max(case when datatype = 'Precipitation' then value end) as precipitation_mm,
    max(case when datatype = 'Average Temperature.' then value end) as average_temperature_c
from weather_base_vw
where station = 'SINGAPORE CHANGI INTERNATIONAL, SN'
  and datatype in ('Precipitation', 'Average Temperature.')
group by date, station
having max(case when datatype = 'Precipitation' then value end) is not null
   and max(case when datatype = 'Precipitation' then value end) > 0
   and max(case when datatype = 'Average Temperature.' then value end) is not null
order by date desc;