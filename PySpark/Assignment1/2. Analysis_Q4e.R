# STAT478-18S1 Assignment 1
# Howard Low, 53626262

library(data.table)  
library(dplyr)
library(ggplot2)
library(plotly)
library(countrycode)
library(qdap)

theme_set(theme_minimal())

files <- list.files(pattern = ".csv")
temp <- lapply(files, fread, sep=",")
data <- rbindlist(temp)
colnames(data) <- c("country_code", "country_name", "year", "average_annual")

maxvalue <- max(data$year)
minvalue <- min(data$year)

DT <- data.table(data) 
df <- DT[ , .(total = mean(average_annual) * 255), by = .(country_name,country_code)]

df$country_name <-  genX(df$country_name, " [", "]")
df$code <- countrycode(df$country_name, 'country.name', 'iso3c')

l <- list(color = toRGB("grey"), width = 0.5)

# specify map projection/options
g <- list(
  showframe = FALSE,
  showcoastlines = FALSE,
  projection = list(type = 'Mercator')
)

p <- plot_geo(df) %>%
  add_trace(
    z = ~total, color = ~total, colors = 'Blues',
    text = ~country_name, locations = ~code, marker = list(line = l)
  ) %>%
  colorbar(title = 'Rainfall (tmm)', tickprefix = '') %>%
  layout(
    title = 'Cumulative Average Annual Rainfall from Year 1781 to 2017',
    geo = g
  )
p

