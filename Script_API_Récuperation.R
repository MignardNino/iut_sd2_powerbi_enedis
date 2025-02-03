
library(httr)
library(readr)
library(dplyr)
library(jsonlite)
library(sf)

#création de certaines variables
annee = seq(2021,2024,1)

#recupère le fichier des adresses :
adresses_48 = read.csv("adresses-69.csv", header = TRUE, sep = ";", dec = ".")
code = unique(adresses_48$code_postal)



#RECUPERE LES LOGEMENTS EXISTANTS
df = data.frame()

base_url <- "https://data.ademe.fr/data-fair/api/v1/datasets/dpe-v2-logements-existants/lines"

ColAGarderExistants = c("Année_construction,Date_réception_DPE,Type_bâtiment,Etiquette_DPE,Etiquette_GES,Coût_total_5_usages,Coût_ECS,Coût_chauffage,Coût_refroidissement,Coût_éclairage,Coordonnée_cartographique_X_(BAN),Coordonnée_cartographique_Y_(BAN),N°DPE,Code_postal_(BAN),Adresse_(BAN),Conso_chauffage_é_finale,Conso_éclairage_é_finale,Conso_refroidissement_é_finale,Conso_ECS_é_finale,Conso_5_usages_é_finale,Emission_GES_chauffage,Emission_GES_éclairage,Emission_GES_refroidissement,Emission_GES_ECS,Emission_GES_5_usages,Qualité_isolation_menuiseries,Qualité_isolation_murs,Qualité_isolation_enveloppe,Qualité_isolation_plancher_bas,Surface_habitable_logement,Surface_habitable_immeuble")

for (i in code) {
  params <- list(
    size = 1,
    q = i,
    q_fields = "Code_postal_(BAN)"
  ) 
  url_encoded <- modify_url(base_url, query = params)
  response <- GET(url_encoded)
  content = fromJSON(rawToChar(response$content), flatten = FALSE)
  
  if (content$total < 10000) {
    params <- list(
      size = 10000,
      q = i,
      q_fields = "Code_postal_(BAN)",
      select = ColAGarderExistants
    ) 
    url_encoded <- modify_url(base_url, query = params)
    response <- GET(url_encoded)
    content = fromJSON(rawToChar(response$content), flatten = FALSE)
    
    df = rbind(df,content$result)}
  
  else { for (a in annee) {
    a=as.character(a)
    params <- list(
      size = 10000,
      q = i,
      q_fields = "Code_postal_(BAN)",
      qs = paste0("Date_réception_DPE:[",a,"-01-01 TO ",a,"-12-31]"),
      select = ColAGarderExistants
    ) 
    
    
    url_encoded <- modify_url(base_url, query = params)
    response <- GET(url_encoded)
    content = fromJSON(rawToChar(response$content), flatten = FALSE)
    
    df = rbind(df,content$result)}
  }
}


#RECUPERE LES LOGEMENTS NEUFS
df2 = data.frame()

ColAGarderNeufs = c("Date_réception_DPE,Type_bâtiment,Etiquette_DPE,Etiquette_GES,Coût_total_5_usages,Coût_ECS,Coût_chauffage,Coût_refroidissement,Coût_éclairage,Coordonnée_cartographique_X_(BAN),Coordonnée_cartographique_Y_(BAN),N°DPE,Code_postal_(BAN),Adresse_(BAN),Conso_chauffage_é_finale,Conso_éclairage_é_finale,Conso_refroidissement_é_finale,Conso_ECS_é_finale,Conso_5_usages_é_finale,Emission_GES_chauffage,Emission_GES_éclairage,Emission_GES_refroidissement,Emission_GES_ECS,Emission_GES_5_usages,Qualité_isolation_menuiseries,Qualité_isolation_murs,Qualité_isolation_enveloppe,Qualité_isolation_plancher_bas,Surface_habitable_logement,Surface_habitable_immeuble")

base_url <- "https://data.ademe.fr/data-fair/api/v1/datasets/dpe-v2-logements-neufs/lines"

for (i in code) {
  params <- list(
    size = 1,
    q = i,
    q_fields = "Code_postal_(BAN)"
  ) 
  url_encoded <- modify_url(base_url, query = params)
  response <- GET(url_encoded)
  content = fromJSON(rawToChar(response$content), flatten = FALSE)
  
  if (content$total < 10000) {
    params <- list(
      size = 10000,
      q = i,
      q_fields = "Code_postal_(BAN)",
      select = ColAGarderNeufs
    ) 
    url_encoded <- modify_url(base_url, query = params)
    response <- GET(url_encoded)
    content = fromJSON(rawToChar(response$content), flatten = FALSE)
    
    df2 = rbind(df2,content$result)}
  
  else { for (a in annee) {
    a=as.character(a)
    params <- list(
      size = 10000,
      q = i,
      q_fields = "Code_postal_(BAN)",
      qs = paste0("Date_réception_DPE:[",a,"-01-01 TO ",a,"-12-31]"),
      select = ColAGarderNeufs
    ) 
    
    
    url_encoded <- modify_url(base_url, query = params)
    response <- GET(url_encoded)
    content = fromJSON(rawToChar(response$content), flatten = FALSE)
    
    df2 = rbind(df2,content$result)}
  }
}
df2$Année_construction = 2024

df$Type_hab = "ancien"
df2$Type_hab = "neuf"

#Assemblé les 2 df :

Lg = rbind(df,df2)

# Convertir les coordonnées cartographiques en objet spatial (pour manipuler des données géographiques)
# Ici, les colonnes Coordonnée_cartographique_X_(BAN) et Coordonnée_cartographique_Y_(BAN) sont utilisées comme coordonnées
coordinates_sf <- st_as_sf(Lg, coords = c("Coordonnée_cartographique_X_(BAN)", "Coordonnée_cartographique_Y_(BAN)"), crs = 2154)

# Transformer les coordonnées au système de référence WGS 84 (latitude/longitude)
coordinates_lat_long <- st_transform(coordinates_sf, crs = 4326)

# Extraire les coordonnées (longitude et latitude) dans un dataframe
lat_long <- st_coordinates(coordinates_lat_long)

# Ajouter les colonnes Longitude et Latitude dans le dataframe principal
Lg$Longitude = lat_long[, 1]
Lg$Latitude = lat_long[, 2]

write.csv(Lg, file = "donnesrhone.csv", row.names = FALSE)