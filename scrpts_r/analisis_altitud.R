
library(RMySQL)
library(tidyverse)
library(job)

###############################################################################
###############################################################################

job::job({



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

})


# Union de consultas ------------------------------------------------------


data_pbc_coord_lote<-rbind(lote_ren_coord_2017, 
                    lote_ren_coord_2018, 
                    lote_ren_coord_2019, 
                    lote_ren_coord_2020, 
                    lote_ren_coord_2021, 
                    lote_ren_coord_2022)


## guardar datos para no volverlos a leer de la base de datos
write.csv(data_pbc_coord_lote, "./data/data_pbc_coord_lote.csv", row.names = F)

################################################################################
################################################################################


# calculo del pbc por ha --------------------------------------------------

data_pbc_coord_lote <- data_pbc_coord_lote %>% mutate(pbc_ha = pbc/area)

# filtrado de datos no validos --------------------------------------------

data_pbc_coord_lote <- data_pbc_coord_lote %>% 
        filter(!is.na(area), c_year!=1999, sem!= 53, area !=0, !is.na(pbc),
               !is.na(pbc))

# total por año y por lote ------------------------------------------------

data_pbc_coord_lote2 <- data_pbc_coord_lote %>% group_by(lote_id, c_year) %>%
        mutate(sum_pbc_ha=sum(pbc_ha)) %>% 
        distinct(sum_pbc_ha, .keep_all=TRUE) %>% ungroup()

# promedio anual por lote -------------------------------------------------

data_pbc_coord_lote3 <- data_pbc_coord_lote2 %>% group_by(lote_id) %>%
        mutate(mean_pbc_anual_ha=mean(sum_pbc_ha)) %>% 
        distinct(mean_pbc_anual_ha, .keep_all=TRUE) %>% ungroup()

# filtrado de lotes sin coordenadas ---------------------------------------

data_pbc_coord_lote3 <- data_pbc_coord_lote3 %>% 
        filter(!is.na(lat)|!is.na(lng))



################################################################################
################################################################################


# Lectura de datos con altura ---------------------------------------------


datos_altura<-read.csv("./data/altitud_lotes.csv")

datos_altura2<-filter(datos_altura, ele<40)

names(datos_altura)

plot(mean_pbc~ele, data = datos_altura2)
        


        










