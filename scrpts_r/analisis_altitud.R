setwd("D:/Sioma/Analisis/analisis_altitud")

library(RMySQL)
library(tidyverse)


###############################################################################
###############################################################################

sioma<-dbConnect(MySQL(), user="sioma_david",
                 host="sioma-app.ciqflvd2anex.us-east-2.rds.amazonaws.com",
                 password ="d7e468d3cbf7c0a343238dbedecb5505", db="sioma_app")

lote_ren_coord_2021<-dbGetQuery(sioma, "select l.lote_id, l.finca_id, l.lat, l.lng, l.area,
sum(v.peso_total) as pbc, date_format(v.fecha,'%v') as sem,
DATE_FORMAT(v.fecha, '%Y') AS c_year
from lotes l
inner join viajes v 
on l.lote_id=v.lote_id
inner join fincas f
on f.finca_id = v.finca_id
WHERE f.tipo_cultivo_id = 1  and v.fecha
between '2021-01-01 00:00:00' AND '2021-12-31 23:59:59'
group by c_year, sem, v.lote_id;")

lote_ren_coord_2022<-dbGetQuery(sioma, "select l.lote_id, l.finca_id, l.lat, l.lng, l.area,
sum(v.peso_total) as pbc, date_format(v.fecha,'%v') as sem,
DATE_FORMAT(v.fecha, '%Y') AS c_year
from lotes l
inner join viajes v 
on l.lote_id=v.lote_id
inner join fincas f
on f.finca_id = v.finca_id
WHERE f.tipo_cultivo_id = 1  and v.fecha
between '2022-01-01 00:00:00' AND '2022-12-31 23:59:59'
group by c_year, sem, v.lote_id;")

lote_ren_coord_2020<-dbGetQuery(sioma, "select l.lote_id, l.finca_id, l.lat, l.lng, l.area,
sum(v.peso_total) as pbc, date_format(v.fecha,'%v') as sem,
DATE_FORMAT(v.fecha, '%Y') AS c_year
from lotes l
inner join viajes v 
on l.lote_id=v.lote_id
inner join fincas f
on f.finca_id = v.finca_id
WHERE f.tipo_cultivo_id = 1  and v.fecha
between '2020-01-01 00:00:00' AND '2020-12-31 23:59:59'
group by c_year, sem, v.lote_id;")

lote_ren_coord_2019<-dbGetQuery(sioma, "select l.lote_id, l.finca_id, l.lat, l.lng, l.area,
sum(v.peso_total) as pbc, date_format(v.fecha,'%v') as sem,
DATE_FORMAT(v.fecha, '%Y') AS c_year
from lotes l
inner join viajes v 
on l.lote_id=v.lote_id
inner join fincas f
on f.finca_id = v.finca_id
WHERE f.tipo_cultivo_id = 1  and v.fecha
between '2019-01-01 00:00:00' AND '2019-12-31 23:59:59'
group by c_year, sem, v.lote_id;")

lote_ren_coord_2018<-dbGetQuery(sioma, "select l.lote_id, l.finca_id, l.lat, l.lng, l.area,
sum(v.peso_total) as pbc, date_format(v.fecha,'%v') as sem,
DATE_FORMAT(v.fecha, '%Y') AS c_year
from lotes l
inner join viajes v 
on l.lote_id=v.lote_id
inner join fincas f
on f.finca_id = v.finca_id
WHERE f.tipo_cultivo_id = 1  and v.fecha
between '2018-01-01 00:00:00' AND '2018-12-31 23:59:59'
group by c_year, sem, v.lote_id;")

lote_ren_coord_2017<-dbGetQuery(sioma, "select l.lote_id, l.finca_id, l.lat, l.lng, l.area,
sum(v.peso_total) as pbc, date_format(v.fecha,'%v') as sem,
DATE_FORMAT(v.fecha, '%Y') AS c_year
from lotes l
inner join viajes v 
on l.lote_id=v.lote_id
inner join fincas f
on f.finca_id = v.finca_id
WHERE f.tipo_cultivo_id = 1  and v.fecha
between '2017-01-01 00:00:00' AND '2017-12-31 23:59:59'
group by c_year, sem, v.lote_id;")

dbDisconnect(sioma) ### importante siempre desconectar

data_pbc_coord_lote<-rbind(lote_ren_coord_2017, 
                    lote_ren_coord_2018, 
                    lote_ren_coord_2019, 
                    lote_ren_coord_2020, 
                    lote_ren_coord_2021, 
                    lote_ren_coord_2022)


## guardar datos para no volverlos a leer de la base de datos
## write.csv(data_pbc_coord_lote, "data_pbc_coord_lote.csv", row.names = F)

################################################################################
################################################################################

## filtrado

#cpha1<-filter(cpha, area.x !=0)
#cpha1<-filter(cpha1, !is.na(area.x))
#cpha1<-filter(cpha1, c_year!=1999)
#cajas_riego_area<-filter(cajas_riego_area, !(c_year==2018 & sem==01))
#cpha1<-filter(cpha1, sem!=53)

############################################################


data_pbc_coord_lote2<- data_pbc_coord_lote %>% group_by(lote_id) %>%
        mutate(mean_pbc=mean(pbc)) %>% 
        distinct(mean_pbc, .keep_all=TRUE) %>% ungroup()

write.csv(data_pbc_coord_lote2, "data_pbc_coord_lote2.csv")        



################################################################################
################################################################################

datos_altura<-read.csv("altitud_lotes.csv")

datos_altura2<-filter(datos_altura, ele<40)

names(datos_altura)

plot(mean_pbc~ele, data = datos_altura2)
        


        










