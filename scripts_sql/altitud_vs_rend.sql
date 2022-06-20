use sioma_app;

select l.lote_id, l.finca_id, l.lat, l.lng, l.area,
sum(v.peso_total) as pbc, date_format(v.fecha,'%v') as sem,
DATE_FORMAT(v.fecha, '%Y') AS c_year
from lotes l
inner join viajes v 
on l.lote_id=v.lote_id
inner join fincas f
on f.finca_id = v.finca_id
WHERE f.tipo_cultivo_id = 1  and v.fecha
between '2021-01-01 00:00:00' AND '2021-12-31 23:59:59'
group by c_year, sem, v.lote_id;
