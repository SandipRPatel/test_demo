-- Databricks notebook source
-- MAGIC %python
-- MAGIC spark

-- COMMAND ----------

create table employees (id int,name string, salary double);

-- COMMAND ----------


select * from employees

-- COMMAND ----------


insert into employees values (1,'sandip',100),(2,'komal',12),(3,'rohit',100),(4,'ajit',100)

-- COMMAND ----------

describe detail employees

-- COMMAND ----------


update employees set salary=salary + 100 where name='sandip'

-- COMMAND ----------


select * from employees

-- COMMAND ----------

describe detail employees

-- COMMAND ----------


describe history employees

-- COMMAND ----------


delete from employees where name = 'komal'

-- COMMAND ----------


select * from employees

-- COMMAND ----------

describe history employees

-- COMMAND ----------

select * from employees version as of 2

-- COMMAND ----------

truncate table employees

-- COMMAND ----------

describe history employees

-- COMMAND ----------

restore table employees version as of 1

-- COMMAND ----------

describe history employees

-- COMMAND ----------

select * from employees

-- COMMAND ----------

describe detail employees

-- COMMAND ----------

optimize employees zorder by id

-- COMMAND ----------

describe detail employees

-- COMMAND ----------

describe history employees

-- COMMAND ----------

vacuum employees retain 0 hours

-- COMMAND ----------

set spark.databricks.delta.retentionDurationCheck.enabled= false

-- COMMAND ----------

vacuum employees retain 0 hours

-- COMMAND ----------

describe history employees

-- COMMAND ----------

select * from employees version as of 0

-- COMMAND ----------

drop table employees

-- COMMAND ----------

describe history employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **managed and external tables**
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC default schema in hive_metastore

-- COMMAND ----------

USE hive_metastore.default;

-- COMMAND ----------

create table managed_hive_table (width int, length int, height int);
insert into managed_hive_table values (1,2,3),(10,20,30),(11,21,31),(14,24,43),(78,45,10);


-- COMMAND ----------

describe extended managed_hive_table

-- COMMAND ----------

create table unmanaged_hive_table (width int, length int, height int)
location 'dbfs:/mnt/demo/test_hive';
-- insert into unmanaged_hive_table values (1,2,3),(10,20,30),(11,21,31),(14,24,43),(78,45,10);


-- COMMAND ----------

describe extended unmanaged_hive_table;

-- COMMAND ----------

insert into unmanaged_hive_table values (1,2,3),(10,20,30),(11,21,31),(14,24,43),(78,45,10);

-- COMMAND ----------

select * from delta.`dbfs:/mnt/demo/test_hive`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('dbfs:/mnt/demo/test_hive'))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC new schema in hive_metastore

-- COMMAND ----------

create schema new_default

-- COMMAND ----------

describe database extended new_default

-- COMMAND ----------

use new_default;

create table managed_new_default (width int, length int, height int);
insert into managed_new_default values (10,42,34),(1,92,34),(1,72,37),(1,26,3),(1,20,3),(31,12,43);

-- COMMAND ----------

describe extended managed_new_default;

-- COMMAND ----------


create table unmanaged_new_default (width int, length int, height int)
location "dbfs:/mnt/demo/unmanaged_new_default";
insert into managed_new_default values (10,42,34),(1,92,34),(1,72,37),(1,26,3),(1,20,3),(31,12,43);

-- COMMAND ----------

describe extended unmanaged_new_default

-- COMMAND ----------

-- MAGIC %md
-- MAGIC new schema outside of hive_metastore

-- COMMAND ----------

create schema custom
location "dbfs:/shared/schemas/custom.db"

-- COMMAND ----------

use custom;

create table managed_custom (width int, length int, height int);
insert into managed_custom values (10,42,34),(1,92,34),(1,72,37),(1,26,3),(1,20,3),(31,12,43);

-- COMMAND ----------


create table unmanaged_custom (width int, length int, height int)
location "dbfs:/mnt/demo/unmanaged_custom";
insert into unmanaged_custom values (10,42,34),(1,92,34),(1,72,37),(1,26,3),(1,20,3),(31,12,43);

-- COMMAND ----------

select * from unmanaged_custom;

-- COMMAND ----------

describe extended managed_custom

-- COMMAND ----------

describe detail managed_custom

-- COMMAND ----------

drop table unmanaged_custom;
select * from delta.`dbfs:/mnt/demo/unmanaged_custom`;

-- COMMAND ----------

drop table managed_custom;
select * from delta.`dbfs:/shared/schemas/custom.db/managed_custom`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **while using otherthan hive_metastore**

-- COMMAND ----------


CREATE TABLE if not exists sunlight_databricks_2025.default.sample2 (width int,height int, weight int)
USING DELTA
LOCATION 'abfss://sandy2025@sunlightadls.dfs.core.windows.net/sample2';
insert into sunlight_databricks_2025.default.sample2 values (10,42,34),(1,92,34),(1,72,37),(1,26,3),(1,20,3),(31,12,43);


-- COMMAND ----------

describe detail sunlight_databricks_2025.default.sample2

-- COMMAND ----------

select * from delta.`abfss://sandy2025@sunlightadls.dfs.core.windows.net/sample2`


-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Views**

-- COMMAND ----------

use hive_metastore.default;
-- Step 1: Create the table
CREATE table emp_persist_view (
  id INT,
  name STRING,
  age INT
);

-- Step 2: Insert 10 rows into the table
INSERT INTO emp_persist_view (id, name, age) VALUES
(1, 'Alice', 25),
(2, 'Bob', 30),
(3, 'Charlie', 35),
(4, 'David', 40),
(5, 'Eve', 28),
(6, 'Frank', 32),
(7, 'Grace', 29),
(8, 'Hank', 45),
(9, 'Ivy', 22),
(10, 'Jack', 27);


-- COMMAND ----------

create view age_25_view as select * from emp_persist_view where age > 25;

-- COMMAND ----------

show views

-- COMMAND ----------

select * from age_25_view

-- COMMAND ----------

create temp view age_25_temp_view as select * from emp_persist_view where age <= 25;

-- COMMAND ----------

select * from age_25_temp_view

-- COMMAND ----------

show views

-- COMMAND ----------

create global temp view age_25_global_view as select * from emp_persist_view where age = 25;

-- COMMAND ----------

show views

-- COMMAND ----------

show tables in global_temp

-- COMMAND ----------

drop view global_temp.age_25_temp_view

-- COMMAND ----------

show tables in global_temp

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **quering flies**

-- COMMAND ----------

-- MAGIC %run /Workspace/Users/sandippatelaero@gmail.com/Databricks-Certified-Data-Engineer-Associate/Includes/Copy-Datasets

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('dbfs:/mnt/demo-datasets/bookstore/customers-json/'))
-- MAGIC
-- MAGIC

-- COMMAND ----------

select * from json.`dbfs:/mnt/demo-datasets/bookstore/customers-json/export_001.json`

-- COMMAND ----------

select * from json.`dbfs:/mnt/demo-datasets/bookstore/customers-json/export_*.json`

-- COMMAND ----------

select count(*) as total_cnt from json.`dbfs:/mnt/demo-datasets/bookstore/customers-json`

-- COMMAND ----------

select *,input_file_name() from json.`dbfs:/mnt/demo-datasets/bookstore/customers-json`

-- COMMAND ----------

select * from text.`dbfs:/mnt/demo-datasets/bookstore/customers-json`

-- COMMAND ----------

select * from binaryFile.`dbfs:/mnt/demo-datasets/bookstore/customers-json`

-- COMMAND ----------

select * from csv.`dbfs:/mnt/demo-datasets/bookstore/books-csv`

-- COMMAND ----------

create table books_csv (book_id string,title string,author string,category string,price float)
using csv
options (header=True,delimiter=';')
location 'dbfs:/mnt/demo-datasets/bookstore/books-csv'

-- COMMAND ----------

select * from hive_metastore.default.books_csv

-- COMMAND ----------

describe extended books_csv

-- COMMAND ----------

create table sunlight_databricks_2025.default.students_csv (empid int,	empname string,	location string,	salary float,	department string,	promoted_or_not int,	joining_date timestamp)
using csv
options (header=True,delimiter=',')
location 'abfss://sandy2025@sunlightadls.dfs.core.windows.net/demo/students_details.csv'

-- COMMAND ----------

select * from sunlight_databricks_2025.default.students_csv

-- COMMAND ----------

describe extended books_csv

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('dbfs:/mnt/demo-datasets/bookstore/books-csv/'))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC spark.read.table("books_csv").write.mode('append').format('csv').option('header','true').option('delimiter',';').save('dbfs:/mnt/demo-datasets/bookstore/books-csv/')
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('dbfs:/mnt/demo-datasets/bookstore/books-csv/'))

-- COMMAND ----------

select count(*) as total_books from books_csv

-- COMMAND ----------

refresh table books_csv;
select count(*) as total_books from books_csv

-- COMMAND ----------

create table customers as select * from json.`dbfs:/mnt/demo-datasets/bookstore/customers-json`

-- COMMAND ----------

describe extended customers

-- COMMAND ----------

create table unparsed_books as select * from csv.`dbfs:/mnt/demo-datasets/bookstore/books-csv`


-- COMMAND ----------

select * from unparsed_books

-- COMMAND ----------

create or replace temp view parsed_books_temp_view (book_id string,title string,author string,category string,price float)
using csv
options (header=True,delimiter=';',path='dbfs:/mnt/demo-datasets/bookstore/books-csv/export_*.csv');
select * from parsed_books_temp_view;


-- COMMAND ----------

describe extended parsed_books_temp_view

-- COMMAND ----------

create table parsed_persisnt_book_csv as select * from parsed_books_temp_view;
describe extended parsed_persisnt_book_csv;

-- COMMAND ----------

create or replace temp view test_books_csv (book_id string,title string,author string,category string,price float)
using csv
options (header=True,delimiter=';',path = 'dbfs:/mnt/demo-datasets/bookstore/books-csv')


-- COMMAND ----------

select * from test_books_csv

-- COMMAND ----------

create table sunlight_databricks_2025.default.address2 (city string, pin int) using delta
location 'abfss://sandy2025@sunlightadls.dfs.core.windows.net/address';

insert into sunlight_databricks_2025.default.address2 values ('New York',12300);
select * from sunlight_databricks_2025.default.address2;

-- COMMAND ----------

describe extended sunlight_databricks_2025.default.address2;

-- COMMAND ----------


