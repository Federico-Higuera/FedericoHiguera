
#Importamos las librerias necesarias
library(dplyr)
library(car)
library(explore)
library(psych)
library(corrplot)


#-------------------------------------

#LEEMOS LOS DATOS
#Cambiar fuente de datos
data = read.csv("C:/Users/FedericoHiguera/OneDrive/Maestrìa/Modelos de Análisis Estadísticos/Competencia/Train real state.csv", sep=",")
data


#CONVERTIMOS LAS VARIABLES CATEGÓRICAS EN DUMMIES
library('fastDummies')
data = dummy_cols(data, select_columns = c("HallwayType",'SubwayStation','HeatingType','AptManageType','TimeToBusStop','TimeToSubway'))
data


#ELIMINAMOS LAS VARIABLES CATEGÓRICAS YA QUE FUERON CONVERTIDAS A DUMMY
borrar = c("HallwayType",'SubwayStation','HeatingType','AptManageType','TimeToBusStop','TimeToSubway')
data_transformada = data[,!(names(data) %in% borrar)]
View(data_transformada)
pairs(data_transformada)


#DIAGRAMA DE DISPERSIÓN DE LAS VARIABLES NUMÉRICAS
library(PerformanceAnalytics)
data_sin_dummies =  data_transformada[,3:25]
View(data_sin_dummies)
pairs(data_sin_dummies)


#-------------------------------------
#MATRIZ DE CORRELACIÓN DE LAS VARIABLES NUMÉRICAS
correlacion<-round(cor(data_sin_dummies), 1)
library('corrplot')
corrplot(correlacion, method="pie", type="full")

#ELIMINAMOS 5 VARIABLES DE ALTA MULTICOLINEALIDAD
borrar_v2 = c("N_FacilitiesNearBy.PublicOffice.",'N_SchoolNearBy.High.','N_SchoolNearBy.University.','N_FacilitiesNearBy.Total.','N_SchoolNearBy.Total.','N_Parkinglot.Basement.')
data_sin_dummies_V2 = data_sin_dummies[,!(names(data_sin_dummies) %in% borrar_v2)]

correlacion_v2<-round(cor(data_sin_dummies_V2), 1)
corrplot(correlacion_v2, method="pie", type="full")


#------------------------------------

#MATRIZ DE CORRELACIÓN DE LAS VARIABLES NUMÉRICAS Y DUMMIES
#round(cor(x = data_transformada, method = "pearson"), 2)



# PRIMER MODELO SIN INCLUIR LAS 5 VARIABLES ELIMINADAS
modelo <- lm(SalePrice ~ YearBuilt + YrSold +	MonthSold +	Size.sqf. + Floor +	N_Parkinglot.Ground. +	N_Parkinglot.Basement. + N_APT +	N_manager +	N_elevators + N_FacilitiesNearBy.PublicOffice. +	N_FacilitiesNearBy.Hospital. +	N_FacilitiesNearBy.Dpartmentstore. + N_FacilitiesNearBy.Mall. +	N_FacilitiesNearBy.ETC. +	N_FacilitiesNearBy.Park. + N_SchoolNearBy.Elementary. +	N_SchoolNearBy.Middle. +	N_SchoolNearBy.High. + N_SchoolNearBy.University. +	N_FacilitiesInApt +	N_FacilitiesNearBy.Total. + N_SchoolNearBy.Total. +	HallwayType_corridor +	HallwayType_mixed + HallwayType_terraced +	SubwayStation_Bangoge +	SubwayStation_Banwoldang + `SubwayStation_Chil-sung-market` +	SubwayStation_Daegu +	SubwayStation_Kyungbuk_uni_hospital + `SubwayStation_Myung-duk` +	SubwayStation_no_subway_nearby +	`SubwayStation_Sin-nam` + HeatingType_central_heating +	HeatingType_individual_heating +	AptManageType_management_in_trust  +	AptManageType_self_management +	`TimeToBusStop_0~5min` +	`TimeToBusStop_5min~10min` + `TimeToBusStop_10min~15min` + `TimeToSubway_0-5min` +	`TimeToSubway_5min~10min` + `TimeToSubway_10min~15min` +	`TimeToSubway_15min~20min` +	TimeToSubway_no_bus_stop_nearby , data = data_transformada )
summary(modelo)
# R2 = 0.88

modelo
#---------------------------------------------------------------------
#---------------------------------------------------------------------
#1. ELECCIÓN DE LAS VARIABLES INDEPENDIENTES
#MÉTODO DE ELIMINACIÓN HACIA ATRAS
library(ggplot2)
library(MASS)
library(leaps)

step.model <- stepAIC(modelo, direction = "backward", 
                      trace = FALSE)
summary(step.model) 
#Aqui se identifican 18 variables con alta dependencia
# Se identifica un R2 = 0.8871

#IDENTIFICACIÓN DE OUTLIERS EN EL MODELO
cooksd<-cooks.distance(modelo_vf)
plot(cooksd, pch="*",cex=2)


outliers <- cooksd < 0.004
sum(outliers)
data_sin_outliers <- data_transformada[outliers,]
data_transformada <- data_sin_outliers


# SEGUNDO MODELO SIN INCLUIR LAS 18 VARIABLES
#Al aplicar este método de eliminación hacia atrás se eliminan las 18 variables de alta dependencia
subset<-regsubsets(SalePrice ~ YearBuilt + YrSold +	MonthSold +	Size.sqf. + Floor +	N_Parkinglot.Ground. +	N_Parkinglot.Basement. + N_APT +	N_manager +	N_elevators + N_FacilitiesNearBy.PublicOffice. +	N_FacilitiesNearBy.Hospital. +	N_FacilitiesNearBy.Dpartmentstore. + N_FacilitiesNearBy.Mall. +	N_FacilitiesNearBy.ETC. +	N_FacilitiesNearBy.Park. + N_SchoolNearBy.Elementary. +	N_SchoolNearBy.Middle. +	N_SchoolNearBy.High. + N_SchoolNearBy.University. +	N_FacilitiesInApt +	HallwayType_corridor +	HallwayType_mixed +	SubwayStation_Bangoge + `SubwayStation_Chil-sung-market` +	SubwayStation_Daegu +	SubwayStation_Kyungbuk_uni_hospital ,data_transformada,method = "backward")
set<-summary(subset)
plot(set$adjr2,type="l")
set
# La gráfica de R2ajustado sugiere utilizar 3 variables 
set$adjr2
# R2 = 0.829

# TERCER MODELO INCLUYENDO SOLO 3 VARIABLES
modelo_vf <- lm(SalePrice ~ YrSold + Size.sqf. + N_FacilitiesInApt ,data = data_transformada)
summary(modelo_vf)

# VERIFICACION DE LINEALIDAD DE LAS VARIABLES INDEPENDIENTES
# Ecuacion lineal < SalePrice ~  0YrSold + Size.sqf. + N_FacilitiesInApt > 


#  - Valor fijo para YrSold: 2009
#  - Valor fijo para SalePrice: 371681
#  - Valor fijo para Size.sqf.: 1519
#  - Valor fijo para N_FacilitiesInApt:4


max(table(data_transformada$Size.sqf.))
(data_transformada$Size.sqf.[673])
sum(data_transformada$Size.sqf. == 1519)


# PARA ANALISIS DE YrSold vs SalePrice
filtro = data_transformada$Size.sqf.== 1519
sum(filtro)
data_transformada$N_FacilitiesInApt[filtro]
plot(data_transformada$YrSold[filtro], data_transformada$SalePrice[filtro])

write.csv(data_transformada$YrSold[filtro], "C:/Users/FedericoHiguera/OneDrive/Maestrìa/Modelos de Análisis Estadísticos/Competencia/Y_1.csv") 
write.csv(data_transformada$SalePrice[filtro], "C:/Users/FedericoHiguera/OneDrive/Maestrìa/Modelos de Análisis Estadísticos/Competencia/X_1.csv") 


# PARA ANALISIS DE Size.sqf.
filtro = data_transformada$YrSold== 2009
table(data_transformada$N_FacilitiesInApt[filtro])
filtro = data_transformada$YrSold== 2009 & data_transformada$N_FacilitiesInApt ==5
plot(data_transformada$Size.sqf.[filtro], data_transformada$SalePrice[filtro])


# PARA ANALISIS DE N_FacilitiesInApt
filtro = data_transformada$YrSold== 2009
max(table(data_transformada$Size.sqf.[filtro]))
filtro = data_transformada$YrSold== 2009 & data_transformada$Size.sqf. == 914
plot(data_transformada$N_FacilitiesInApt[filtro], data_transformada$SalePrice[filtro])



#---------------------------------------------------------------------
#2. Determinación de la estructura del modelo
#3. Estimación de los parámetros del modelo
summary(modelo_vf)


#---------------------------------------------------------------------
#4. Verificación de los supuestos del modelo 

#TRASFORMACION

cox <- boxcox(modelo_vf, plotit = TRUE, data = data_transformada)
lambda <- cox$x[which.max(cox$y)]

modeloCox <-lm((((SalePrice ^ lambda) - 1) / lambda) ~ YrSold + Size.sqf. + Floor + N_FacilitiesNearBy.Dpartmentstore. + N_FacilitiesNearBy.Park. + N_SchoolNearBy.High. + N_FacilitiesInApt + `SubwayStation_Chil-sung-market`,data = data_transformada)

modelo_vf=modeloCox


#..............................
# MEDIA CERO
residuos<-modelo_vf$residuals

plot(y=residuos,x=data_transformada$YrSold )
plot(y=residuos,x=data_transformada$Size.sqf. )
plot(y=residuos,x=data_transformada$N_FacilitiesInApt )
#plot(y=residuos,x=data_transformada$Floor )#
#plot(y=residuos,x=data_transformada$N_FacilitiesNearBy.Dpartmentstore. )
#plot(y=residuos,x=data_transformada$N_FacilitiesNearBy.Park.  )
#plot(y=residuos,x=data_transformada$N_SchoolNearBy.High. )#
#plot(y=residuos,x=data_transformada$`SubwayStation_Chil-sung-market` )
#plot(y=residuos,x=data_transformada$N_APT )


#..............................
# VARIANZA CONSTANTE
residuos<-modelo_vf$residuals
proyectados<-modelo_vf$fitted.values

plot(y=residuos,x=proyectados)
plot(modelo_vf, 3)
library("lmtest")


#Ahora realizamos el Test de Breusch-Pagan para constrastar las dos hipótesis
#Ho: los errores tienen una varianza constante
#Ha: los errores no tienen una varianza constante

bptest(modelo_vf)
qchisq(0.95,2)



#..............................
#* NORMALIDAD
residuos<-modelo_vf$residuals
par(mfrow=c(1,1))

qqnorm(residuos)
qqline(residuos)
hist(residuos)


#..............................
#*NO MULTICOLINEALIDAD
vif(modelo_vf)


#---------------------------------------------------------------------
#5. Determinación de la bondad de ajuste del modelo (5 puntos)
summary(modelo_vf)


#---------------------------------------------------------------------
#6. Cálculo de intervalos de confianza para los parámetros del modelo (5 puntos)
confint(modelo_vf)
#Intervalos de confianza del 95% 

