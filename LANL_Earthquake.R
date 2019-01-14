#  Big data problem  #

# https://www.kaggle.com/c/LANL-Earthquake-Prediction
# use seismic signals to predict the timing of laboratory earthquakes.
# The training data is a single, continuous segment of experimental data. The test data consists of a folder containing many small segments. The data within each test file is continuous, but the test files do not represent a continuous segment of the experiment; thus, the predictions cannot be assumed to follow the same regular pattern seen in the training file.

library(tidyverse)

train10lines=read.csv("../LANLEarthquakeData/train.csv", nrows=10)
train1000lines=read.csv("../LANLEarthquakeData/train.csv", nrows=1000)

# 1. using readr function: read_csv_chunked, faster than read_csv, but memory is not enough, dead computer #
f <- function(x, pos) subset(x, time_to_failure >= 0) 
train=read_csv_chunked("../LANLEarthquakeData/train.csv", callback=DataFrameCallback$new(f), chunk_size = 10000, col_names = TRUE)

# 2. using data.table, fastest, but use memory 7.55GB, not possible to do any work afterwards
library(data.table)
train = fread('../LANLEarthquakeData/train.csv', header = T, sep = ',')
library(pryr)
object_size(1:10)  #96 B
object_size(train)  #7.55 GB
object_size(train10lines) #1.04 kB
object_size(train1000lines) #12.9 kB

# 3. using ff, slower than fread, but faster than read_csv, but use much less memory, computer is still functioning.
# One advantage ff has over bigmemory is that it supports multiple data class types in the data set unlike bigmemory.
library(ff)
train = read.csv.ffdf(file = '../LANLEarthquakeData/train.csv', header = T)
object_size(1:10)  #96 B
object_size(train)  #4.53 kB
library(ffbase)
colnames(train)
summary(train)
summary(train$time_to_failure) # crashed on this step

# 4. using bigmemory.  
# the data set has to be only one class of data.
library(bigmemory)
train = read.big.matrix('../LANLEarthquakeData/train.csv', header = T)

# 5. using sqldf
# install.packages("sqldf")
library(sqldf)
system.time(read.csv.sql('../LANLEarthquakeData/train.csv'))   #profile time elpased
train = read.csv.sql('../LANLEarthquakeData/train.csv')

# 6. read in chunks
#https://stackoverflow.com/questions/45362126/still-struggling-with-handling-large-data-set
# Define only the subset of columns
csv <- "my.csv"
colnames <- names(read.csv(csv, header = TRUE, nrows = 1))
colclasses <- rep(list(NULL), length(colnames))
ind <- c(1, 2, 7, 12, 15)
colclasses[ind] <- "double"

# Read header and first line
library(dplyr)
l_df <- list()
con <- file(csv, "rt")
df <- read.csv(con, header = TRUE, nrows = 1, colClasses = colclasses) %>%
  filter(V1 == 6, V7 == 1)
names(df) <- paste0("V", ind)
l_df[[i <- 1]] <- df

# Read all other lines and combine
repeat {
  i <- i + 1
  df <- read.csv(con, header = FALSE, nrows = 9973, colClasses = colclasses)
  l_df[[i]] <- filter(df, V1 == 6, V7 == 1)
  if (nrow(df) < 9973) break
}
df <- do.call("rbind", l_df)
#


# old method #
# train = read_csv("../LANLEarthquakeData/train.csv")
save(train, file="train.RData")






# analysis with subset of data, when memory is not enough 
#http://www.columbia.edu/~sjm2186/EPIC_R/EPIC_R_BigData.pdf
rows <- [1:500]
columns <- [1:30]
subset <- bigdata[rows, columns]
rm(bigdata)



## profile time used by each method of importing large csv file ##
## from R-bloggers ##

### size of csv file: 689.4MB (7,009,728 rows * 29 columns) ###
 
system.time(read.csv('../data/2008.csv', header = T))
#   user  system elapsed 
# 88.301   2.416  90.716
 
library(data.table)
system.time(fread('../data/2008.csv', header = T, sep = ',')) 
#   user  system elapsed 
#  4.740   0.048   4.785
 
library(bigmemory)
system.time(read.big.matrix('../data/2008.csv', header = T))
#   user  system elapsed 
# 59.544   0.764  60.308
 
library(ff)
system.time(read.csv.ffdf(file = '../data/2008.csv', header = T))
#   user  system elapsed 
# 60.028   1.280  61.335 
 
library(sqldf)
system.time(read.csv.sql('../data/2008.csv'))
#   user  system elapsed 
# 87.461   3.880  91.447