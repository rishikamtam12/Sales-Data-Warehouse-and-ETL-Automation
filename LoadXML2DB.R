# ---------------------------------------
# Title: Create Analytics Database
# Author: Rishi Kamtam
# ---------------------------------------


# ERD for relational schema:
# https://lucid.app/lucidchart/28c476fe-c0df-4f4b-8ca9-17789a423747/edit?viewport_loc=-99%2C-25%2C1996%2C1033%2C0_0&invitationId=inv_4b373cf3-de2c-42b6-a1c5-17a6ddeed9ca



# loading necessary libraries
library(RSQLite)
library(data.table)

# connecting to database
dbcon <- dbConnect(RSQLite::SQLite(), "final_project.db")

# enabling foreign key constraints 
dbExecute(dbcon, "PRAGMA foreign_keys = ON")


# dropping the tables so code can be re run 
dbExecute(dbcon, "DROP TABLE IF EXISTS sales_2020")
dbExecute(dbcon, "DROP TABLE IF EXISTS sales_2021")
dbExecute(dbcon, "DROP TABLE IF EXISTS sales_2022")
dbExecute(dbcon, "DROP TABLE IF EXISTS sales_2023")
dbExecute(dbcon, "DROP TABLE IF EXISTS sales")
dbExecute(dbcon, "DROP TABLE IF EXISTS reps")
dbExecute(dbcon, "DROP TABLE IF EXISTS products")
dbExecute(dbcon, "DROP TABLE IF EXISTS customers")


###
### creating tables
###

# creating products table
create_prod_sql <- "
CREATE TABLE IF NOT EXISTS products (
  prodID INTEGER NOT NULL,
  prod TEXT NOT NULL,
  PRIMARY KEY (prodID)
  );"

# creating reps table
create_reps_sql <- "
CREATE TABLE IF NOT EXISTS reps (
  repID INTEGER NOT NULL,
  repFN TEXT NOT NULL,
  repLN TEXT NOT NULL,
  repTR TEXT NOT NULL,
  repPh TEXT NOT NULL,
  repCm REAL NOT NULL,
  repHireDate DATE,
  PRIMARY KEY (repID)
  );"

# creating customers table
create_customers_sql <- "
CREATE TABLE IF NOT EXISTS customers(
  custID INTEGER NOT NULL,
  cust TEXT NOT NULL,
  country TEXT NOT NULL,
  PRIMARY KEY(custID)
  );"

# creating sales table
create_sales_sql <- "
CREATE TABLE IF NOT EXISTS sales(
  salesID INTEGER NOT NULL,
  date DATE NOT NULL,
  qty INTEGER NOT NULL,
  unitcost REAL NOT NULL,
  custID INTEGER NOT NULL,
  prodID INTEGER NOT NULL,
  repID INTEGER NOT NULL,
  FOREIGN KEY (custID) REFERENCES customers(custID),
  FOREIGN KEY (prodID) REFERENCES products(prodID),
  FOREIGN KEY (repID) REFERENCES reps(repID)
  );"


# executing the creation of all the tables
dbExecute(dbcon, create_prod_sql)
dbExecute(dbcon, create_reps_sql)
dbExecute(dbcon, create_customers_sql)
dbExecute(dbcon, create_sales_sql)




###
### extracting, transforming, and loading the data into the database
### 
### Note: Converting dates to YYYY-MM-DD format as it is widely supported
### Planning on converting dates while handling the data
###

# creating a list of files with same naming pattern
rep_files <- list.files(path = "csv-data", pattern = "pharmaReps.*\\.csv$",
                        full.names = TRUE)
txn_files <- list.files(path = "csv-data", pattern = "pharmaSalesTxn.*\\.csv$",
                        full.names = TRUE)


# reading and combining (using rbindlist) all rep files into a single data.table
rep_data_list <- lapply(rep_files, fread)
reps_dt <- rbindlist(rep_data_list)

# converting the rep hire date from to YYYY-MM-DD format
# := operater used to update/create a column in a data table
reps_dt[, repHireDate := format(as.Date(repHireDate, format = "%b %d %Y"),
                                "%Y-%m-%d")]

# reading and combining all transaction files into a single data.table
txn_data_list <- lapply(txn_files, fread)
txn_dt <- rbindlist(txn_data_list)

# extracting unique customers from transaction data and assigning a customer ID 
customers_dt <- txn_dt[, .(cust, country)]
customers_dt <- unique(customers_dt, by = "cust")
customers_dt[, custID := .I]

# extracting unique products from transaction data and assigning a product ID
products_dt <- txn_dt[, .(prod)]
products_dt <- unique(products_dt, by = "prod")
products_dt[, prodID := .I]


# merging txn_dt with customers_dt and products_dt to add custID and prodID
txn_dt <- merge(txn_dt, customers_dt[, .(cust, custID)], by = "cust",
                all.x = TRUE)
txn_dt <- merge(txn_dt, products_dt[, .(prod, prodID)], by = "prod",
                all.x = TRUE)


# generating sales_id for each transaction and converting date to new format
sales_dt <- txn_dt[, .(date = format(as.Date(date, format = "%m/%d/%Y"),
                                     "%Y-%m-%d"),
                       qty, unitcost, custID, prodID, repID)]
sales_dt[, salesID := .I]


# writing data to the respective tables
dbWriteTable(dbcon, "reps", reps_dt, append = TRUE, row.names = FALSE)
dbWriteTable(dbcon, "customers", customers_dt, append = TRUE, row.names = FALSE)
dbWriteTable(dbcon, "products", products_dt, append = TRUE, row.names = FALSE)
dbWriteTable(dbcon, "sales", sales_dt, append = TRUE, row.names = FALSE)


###
###
### Date range partitioning on sales table
###
###

# extracting the years found in the sales table based on date column
years <- dbGetQuery(dbcon, "SELECT DISTINCT strftime('%Y', sales.date) AS year 
                    FROM sales ORDER BY year;")
# converting the years into a list for easier iteration
years <- as.list(years$year)

# looping through the list of years to create a new table for each of them
for (year in years) {
  # creating the name of the new table for each year
  table_name <- paste0("sales_", year)

  # creating the new table for the given year
  # query within create table statement populates tables based on given year
  dbExecute(dbcon, paste0("
    CREATE TABLE IF NOT EXISTS ", table_name, " AS
    SELECT * FROM sales WHERE strftime('%Y', date) = '", year, "';
  "))
}


# removing large sales table 
dbExecute(dbcon, "DROP TABLE IF EXISTS sales;")


# disconnecting from database
dbDisconnect(dbcon)




