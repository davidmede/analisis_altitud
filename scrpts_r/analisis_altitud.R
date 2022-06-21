
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


# creacion de archivo para añadirle la altitud ----------------------------

write.csv(data_pbc_coord_lote3, "./data/puntos_lotes_rend.csv", row.names = F)

################################################################################
################################################################################


# analisis de datos con altura ---------------------------------------------


datos_altura<-read.csv("./data/altitud_lotes.csv")

datos_altura2<-filter(datos_altura, mean_pbc_a <70000)

boxplot(datos_altura2$mean_pbc_a)
hist(datos_altura2$mean_pbc_a)


plot(mean_pbc_a~id_altitud_ele, data = datos_altura2)
        
plot(mean_pbc_a~lat, data = datos_altura2)

plot(mean_pbc_a~lng, data = datos_altura2)

#### regresion cuadratica ######

datos_altura2$id_altitud_ele2<-(datos_altura2$id_altitud_ele)^2

aso_lm<-lm(mean_pbc_a~id_altitud_ele+id_altitud_ele2, data = datos_altura2)


summary(aso_lm)

fitted(lm(mean_pbc_a~id_altitud_ele+id_altitud_ele2, data = datos_altura2))

psf_prom<-tapply(datos_altura2$mean_pbc_a, 
                 datos_altura2$id_altitud_ele, mean)

plot(mean_pbc_a~id_altitud_ele, data = datos_altura2)
lines(psf_prom, 
     fitted(lm(mean_pbc_a~id_altitud_ele+id_altitud_ele2, data = datos_altura2)))


############################ regresion  gompertz #######################

library(drc)
###gompertz
g_rend<-drm(mean_pbc_a ~ id_altitud_ele, fct = G.3(), data=datos_altura2)

summary(g_rend)

plot(g_rend)


#####predicciom

p_gmp<-function(mod=mod, ele=ele){
        b<-coef(mod)
        p_y<-b[2]*exp(-exp(b[1]*(ele-b[3])))
        return(p_y)
}


rend_p<-p_gmp(mod = g_rend, ele = datos_altura2$id_altitud_ele)

a<-cbind(datos_altura2$mean_pbc_a, 
      p_gmp(mod = g_rend, ele = datos_altura2$id_altitud_ele))

#### ajuste


rmspd<-function(yo=yo,yp=yp) sqrt(sum((yp-yo)^2)/length(yo))

rmspd(yo = datos_altura2$mean_pbc_a, yp = rend_p)

r_2<-function(yo=yo,yp=yp){
        r_2<-1-(sum((yo-yp)^2)/sum((yo-mean(yo))^2))
        return(r_2)
}
r_2(datos_altura2$mean_pbc_a, rend_p) 



############ validacion cruzada #################################


muestra<-sample(1:54,5)
val_fol<-rug_foliar[muestra,]
ent_fol<-rug_foliar[-c(muestra),]

g_rug_fol<-drm(p.psf~dia, fct = G.3(), data=ent_fol)
summary(g_rug_fol)


pred_gomp<-function(mod=mod, t=t){
        b<-coef(mod)
        py<-b[2]*exp(-exp(b[1]*(t-b[3])))
        return(py)
}

yp<-pred_gomp(t=val_fol$dia,mod=g_rug_fol)

rmspd<-function(yo=yo,yp=yp) sqrt(sum((yp-yo)^2)/length(yo))

rmspd(val_fol$p.psf,yp)

r_2<-function(yo=yo,yp=yp){
        r_2<-1-(sum((yo-yp)^2)/sum((yo-mean(yo))^2))
        return(r_2)
}
r_2(val_fol$p.psf,yp)

r_2_adj<-function(r_2=r_2,n=n,p=p){
        adj<-1-(1-r_2)*(n-1)/(n-p-1)
        return(adj)
}
r_2_adj()  


#### analisis de un modelo



 





# analisis longitud -------------------------------------------------------




# analisis altitud (todos los puntos) -------------------------------------

datos_altura_todos<-read.csv("./data/altitud_lotes_todos.csv")

datos_altura_todos2<-filter(datos_altura_todos, puntos_todos_id_ele < 500, 
                            mean_pbc_a <70000 )

plot(mean_pbc_a~lat, data = datos_altura_todos2)
plot(mean_pbc_a~lng, data = datos_altura_todos2)








