#import library
library(DBI)
library(dplyr)

setwd("Downloads/ST2195 Assignment/Assignment 3") #setting work directory to file location

#Creating DB file in working directory for SQL
if (file.exists("airline2.db"))
  file.remove("airline2.db")

#run remove code first to prevent error when connecting to db

conn <-dbConnect(RSQLite::SQLite(), "airline2.db") #connect to db

#======= write to database ==========
airports <-read.csv("airports.csv", header=TRUE)
carriers <-read.csv("carriers.csv", header=TRUE)
planes <-read.csv("plane-data.csv", header=TRUE)

dbWriteTable(conn,"Airports",airports)
dbWriteTable(conn,"Carrier",carriers)
dbWriteTable(conn,"Planes",planes)  

for(i in c(2000:2005)) {  
  ontime<-read.csv(paste0(i,".csv"), header=TRUE)
  if(i == 2000){  
    dbWriteTable(conn,"ontime",ontime)
  } else {
    dbWriteTable(conn,"ontime",ontime, append= TRUE)
  }
}
  
#====queries via DBI=====
q1<-dbGetQuery(conn,
  "SELECT model AS model,AVG(ontime.DepDelay) AS avg_delay
  FROM planes JOIN ontime USING(tailnum)
  WHERE ontime.Cancelled = 0 AND ontime.Diverted = 0 AND ontime.DepDelay > 0
  GROUP by model
  ORDER by avg_delay"           
)

print(paste(q1[1,"model"],"has the lowest associated average departure delay."))

print(q1)

q2 <- dbGetQuery(conn,
  "SELECT airports.city AS city, COUNT(*) AS total
  FROM airports JOIN ontime ON ontime.dest = airports.iata
  WHERE ontime.Cancelled = 0
  GROUP by airports.city
  ORDER by total DESC"
)
 
print(paste(q2[1,"city"],"has the highest number of inbound flights(excluding cancelled flights)"))

q3 <- dbGetQuery(conn,
  "SELECT carrier.Description AS carrier, COUNT(*) AS total
  FROM carrier JOIN ontime ON ontime.UniqueCarrier = carrier.Code
  WHERE ontime.Cancelled = 1 
      AND carrier.Description IN ('Delta Air Lines Inc.', 'United Air Lines Inc.', 'American Airlines Inc.','Pinnacle Airlines Inc.')
  GROUP by carrier.Description
  ORDER by total DESC"
  )

print(paste(q3[1,"carrier"], "has the highest number of cancelled flight"))

#=======query via dbpylr==========

airports_db <-tbl(conn,"airports")
carriers_db <- tbl(conn,"carrier")
planes_db <- tbl(conn,"planes")
ontime_db <- tbl(conn,"ontime")


q1dbpylr <- ontime_db %>%
    rename_all(tolower) %>%
    inner_join(planes_db, by ="tailnum", suffix=c(".ontime" , ".planes")) %>%
    filter(Cancelled == 0 & Diverted == 0 & DepDelay > 0) %>%
    group_by(model) %>%
    summarize(avg_delay = mean(DepDelay,na.rm = TRUE)) %>%
    arrange(avg_delay)
    

print(head(q1dbpylr,1))

q2dbplyr <- ontime_db %>%
    inner_join(airports_db, by = c("Dest"="iata")) %>%
    filter(Cancelled == 0) %>%
    group_by(city) %>%
    summarize(total = n()) %>%
      arrange(desc(total))

print(head(q2dbplyr,1))

