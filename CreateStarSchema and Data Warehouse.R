# ---------------------------------------
# Title: Create Star Schema and Data Warehouse
# Author: Rishi Kamtam
# ---------------------------------------


# loading the necessary libaries
library(RMySQL)

# defining parameters for MySQL database connection
db_name <- "sql5726266"
db_user <- "sql5726266"
db_host <- "sql5.freesqldatabase.com"
db_pwd <- "YeQH7DryPF"
db_port <- 3306

# establishing connection to MySQL database
mysql_dbcon <-  dbConnect(RMySQL::MySQL(), user = db_user, password = db_pwd,
                      dbname = db_name, host = db_host, port = db_port)

# connecting to SQLite database
sqlite_dbcon <- dbConnect(RSQLite::SQLite(), "final_project.db")


# dropping the tables so code can be re run 
dbExecute(mysql_dbcon, "DROP TABLE IF EXISTS product_facts")
dbExecute(mysql_dbcon, "DROP TABLE IF EXISTS time_dim")
dbExecute(mysql_dbcon, "DROP TABLE IF EXISTS location_dim")



###
### Creating fact and dimension tables
###

# creating product fact table
create_prod_facts_sql <- "
CREATE TABLE IF NOT EXISTS product_facts(
  fact_id INTEGER PRIMARY KEY AUTO_INCREMENT NOT NULL,
  product_name TEXT NOT NULL,
  total_amount REAL NOT NULL,
  total_units INTEGER NOT NULL,
  time_id INTEGER NOT NULL,
  location_id INTEGER NOT NULL,
  FOREIGN KEY (time_id) REFERENCES time_dim(time_id),
  FOREIGN KEY (location_id) REFERENCES location_dim(location_id)
    );"

# creating time dimension table
create_time_dim_sql <- "
CREATE TABLE IF NOT EXISTS time_dim(
  time_id INT PRIMARY KEY AUTO_INCREMENT,
  year INT NOT NULL,
  month INT NOT NULL
);"

# creating location dimension table
create_location_dim_sql <- "
CREATE TABLE IF NOT EXISTS location_dim(
  location_id INTEGER PRIMARY KEY AUTO_INCREMENT,
  location_name TEXT NOT NULL
);"

# executing creation of the tables
dbExecute(mysql_dbcon, create_time_dim_sql)
dbExecute(mysql_dbcon, create_location_dim_sql)
dbExecute(mysql_dbcon, create_prod_facts_sql)


###
### Populating data into dimension tables
###

# getting a list of the sales tables for each year from sqlite db
sales_tables <- dbGetQuery(sqlite_dbcon, "
  SELECT name FROM sqlite_master 
  WHERE type='table' AND name LIKE 'sales_%';
")$name


# creating empty data frame for time_dim table
time_data <- data.frame()

# looping through sales_tables and getting the data for time_dim table
for (sales_table in sales_tables) {
  # querying needed data for time_dim table
  query <- paste0("
    SELECT DISTINCT CAST(strftime('%Y', date) AS INTEGER) AS year, 
                    CAST(strftime('%m', date) AS INTEGER) AS month
    FROM ", sales_table, "
    ORDER BY year, month;
  ")
  # adding queried data to data frame
  time_data <- rbind(time_data, dbGetQuery(sqlite_dbcon, query))
}

# inserting data into time dim table
dbWriteTable(mysql_dbcon, "time_dim", time_data, append = TRUE,
             row.names = FALSE)




# creating empty data frame for location_dim table
location_data <- data.frame()

# looping through sales_tables and getting the data for location_dim table
for (sales_table in sales_tables) {
  # querying needed data for location_dim table
  query <- paste0("
    SELECT DISTINCT country AS location_name
    FROM ", sales_table, " 
    JOIN customers ON ", sales_table, ".custID = customers.custID
    ORDER BY location_name;
  ")
  
  # adding queried data to data frame
  location_data <- rbind(location_data, dbGetQuery(sqlite_dbcon, query))
}

# removing duplicate location names 
location_data <- unique(location_data)

# inserting data into location dim table
dbWriteTable(mysql_dbcon, "location_dim", location_data, append = TRUE,
             row.names = FALSE)



###
### Populating data into fact table
###

# looping through sales_tables and getting data for product_facts table
for (sales_table in sales_tables) {
  # querying and aggregating needed data for product_facts table
  query <- paste0("
    SELECT 
      p.prod AS product_name,
      SUM(s.qty * s.unitcost) AS total_amount,
      SUM(s.qty) AS total_units,
      strftime('%Y', s.date) AS year,
      strftime('%m', s.date) AS month,
      c.country AS location_name
    FROM ", sales_table, " s
    JOIN customers c ON s.custID = c.custID
    JOIN products p ON s.prodID = p.prodID
    GROUP BY product_name, year, month, location_name;
  ")
  
  # running and storing the query
  product_facts_data <- dbGetQuery(sqlite_dbcon, query)
  
  # converting month to integer to match time_dim
  product_facts_data$month <- as.integer(product_facts_data$month) 
  
  
  # merging product_facts and time_dim to add the time_id based on yr and month
  time_dim <- dbGetQuery(mysql_dbcon, "SELECT * FROM time_dim")
  product_facts_data <- merge(
    product_facts_data, 
    time_dim, 
    by = c("year", "month")
  )
  
  # merging product_facts and location_dim to add the location_id based on name
  location_dim <- dbGetQuery(mysql_dbcon, "SELECT * FROM location_dim")
  product_facts_data <- merge(
    product_facts_data, 
    location_dim, 
    by = "location_name"
  )
  
  # selecting the columns needed for the fact table
  product_facts_data <- product_facts_data[, c("product_name", "total_amount",
                                      "total_units", "time_id", "location_id")]
  
  # ordering the data by time_id and location_id
  product_facts_data <- product_facts_data[order(product_facts_data$time_id,
                                            product_facts_data$location_id), ]
  
  # inserting the data into the product_facts table
  dbWriteTable(mysql_dbcon, "product_facts", product_facts_data, append = TRUE,
               row.names = FALSE)
}



# disconnecting from databases
dbDisconnect(mysql_dbcon)
dbDisconnect(sqlite_dbcon)
  
