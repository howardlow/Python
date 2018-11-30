# STAT478-18S1 Assignment 1
# Howard Low, 53626262

library(data.table)  
library(dplyr)
library(tidyr)
library(ggplot2)
library(magrittr)
library(anydate)
theme_set(theme_minimal())

files <- list.files(pattern = ".csv")

temp <- lapply(files, fread, sep=",")
data <- rbindlist( temp )
colnames(data) <- c("station_code", "date", "element", "value", "measurement_flag", "quality_flag", "source_flag", "observation_time", "year")

newdata <- data %>% group_by(station_code, year, element) %>% summarize(value = mean(value)) 

#NZ000936150
NZ000936150 <- filter(newdata, station_code == "NZ000936150")
ggplot(NZ000936150, aes(x = year, y = value)) + 
  geom_line(aes(color = element), size = 1) +
  scale_color_manual(values = c("#00AFBB", "#E7B800")) +
  theme_minimal() +
  ggtitle("Annual Rainfall for NZ000936150")  

#NZ000093012
NZ000093012 <- filter(newdata, station_code == "NZ000093012")
ggplot(NZ000093012, aes(x = year, y = value)) + 
  geom_line(aes(color = element), size = 1) +
  scale_color_manual(values = c("#00AFBB", "#E7B800")) +
  theme_minimal() +
  ggtitle("Annual Rainfall for NZ000093012")

#NZ000093292
NZ000093292 <- filter(newdata, station_code == "NZ000093292")
ggplot(NZ000093292, aes(x = year, y = value)) + 
  geom_line(aes(color = element), size = 1) +
  scale_color_manual(values = c("#00AFBB", "#E7B800")) +
  theme_minimal() +
  ggtitle("Annual Rainfall for NZ000093292")

#NZM00093678
NZM00093678 <- filter(newdata, station_code == "NZM00093678")
ggplot(NZM00093678, aes(x = year, y = value)) + 
  geom_line(aes(color = element), size = 1) +
  scale_color_manual(values = c("#00AFBB", "#E7B800")) +
  theme_minimal() +
  ggtitle("Annual Rainfall for NZM00093678")


#NZ000093844
NZ000093844 <- filter(newdata, station_code == "NZ000093844")
ggplot(NZ000093844, aes(x = year, y = value)) + 
  geom_line(aes(color = element), size = 1) +
  scale_color_manual(values = c("#00AFBB", "#E7B800")) +
  theme_minimal() +
  ggtitle("Annual Rainfall for NZ000093844")


#NZ000093417
NZ000093417 <- filter(newdata, station_code == "NZ000093417")
ggplot(NZ000093417, aes(x = year, y = value)) + 
  geom_line(aes(color = element), size = 1) +
  scale_color_manual(values = c("#00AFBB", "#E7B800")) +
  theme_minimal() +
  ggtitle("Annual Rainfall for NZ000093417")


#NZ000933090
NZ000933090 <- filter(newdata, station_code == "NZ000933090")
ggplot(NZ000933090, aes(x = year, y = value)) + 
  geom_line(aes(color = element), size = 1) +
  scale_color_manual(values = c("#00AFBB", "#E7B800")) +
  theme_minimal() +
  ggtitle("Annual Rainfall for NZ000933090")


#NZ000093994
NZ000093994 <- filter(newdata, station_code == "NZ000093994")
ggplot(NZ000093994, aes(x = year, y = value)) + 
  geom_line(aes(color = element), size = 1) +
  scale_color_manual(values = c("#00AFBB", "#E7B800")) +
  theme_minimal() +
  ggtitle("Annual Rainfall for NZ000093994")


#NZM00093110
NZM00093110 <- filter(newdata, station_code == "NZM00093110")
ggplot(NZM00093110, aes(x = year, y = value)) + 
  geom_line(aes(color = element), size = 1) +
  scale_color_manual(values = c("#00AFBB", "#E7B800")) +
  theme_minimal() +
  ggtitle("Annual Rainfall for NZM00093110")


#NZ000937470
NZ000937470 <- filter(newdata, station_code == "NZ000937470")
ggplot(NZ000937470, aes(x = year, y = value)) + 
  geom_line(aes(color = element), size = 1) +
  scale_color_manual(values = c("#00AFBB", "#E7B800")) +
  theme_minimal() +
  ggtitle("Annual Rainfall for NZ000937470")


#NZ000939870
NZ000939870 <- filter(newdata, station_code == "NZ000939870")
ggplot(NZ000939870, aes(x = year, y = value)) + 
  geom_line(aes(color = element), size = 1) +
  scale_color_manual(values = c("#00AFBB", "#E7B800")) +
  theme_minimal() +
  ggtitle("Annual Rainfall for NZ000939870")


#NZM00093781
NZM00093781 <- filter(newdata, station_code == "NZM00093781")
ggplot(NZM00093781, aes(x = year, y = value)) + 
  geom_line(aes(color = element), size = 1) +
  scale_color_manual(values = c("#00AFBB", "#E7B800")) +
  theme_minimal() +
  ggtitle("Annual Rainfall for NZM00093781")



#NZ000939450
NZ000939450 <- filter(newdata, station_code == "NZ000939450")
ggplot(NZ000939450, aes(x = year, y = value)) + 
  geom_line(aes(color = element), size = 1) +
  scale_color_manual(values = c("#00AFBB", "#E7B800")) +
  theme_minimal() +
  ggtitle("Annual Rainfall for NZ000939450")



#NZM00093929
NZM00093929 <- filter(newdata, station_code == "NZM00093929")
ggplot(NZM00093929, aes(x = year, y = value)) + 
  geom_line(aes(color = element), size = 1) +
  scale_color_manual(values = c("#00AFBB", "#E7B800")) +
  theme_minimal() +
  ggtitle("Annual Rainfall for NZM00093929")


#NZM00093439
NZM00093439 <- filter(newdata, station_code == "NZM00093439")
ggplot(NZM00093439, aes(x = year, y = value)) + 
  geom_line(aes(color = element), size = 1) +
  scale_color_manual(values = c("#00AFBB", "#E7B800")) +
  theme_minimal() +
  ggtitle("Annual Rainfall for NZM00093439")


#overall
ggplot(newdata, aes(x = year, y = value)) + 
  geom_line(aes(color = element), size = 1) +
  scale_color_manual(values = c("#00AFBB", "#E7B800")) +
  theme_minimal() +
  ggtitle("Annual Rainfall for All New Zealand Stations")

 

 