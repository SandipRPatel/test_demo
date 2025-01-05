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

-- MAGIC %python
-- MAGIC spark

-- COMMAND ----------

use hive_metastore.default

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('dbfs:/mnt/demo-datasets/bookstore'))
-- MAGIC
-- MAGIC

-- COMMAND ----------

create table orders as select * from parquet.`dbfs:/mnt/demo-datasets/bookstore/orders`;
select * from orders;

-- COMMAND ----------

describe extended orders;

-- COMMAND ----------

create or replace table orders as select * from parquet.`dbfs:/mnt/demo-datasets/bookstore/orders`;
describe history orders;

-- COMMAND ----------

insert overwrite orders
select * from parquet.`dbfs:/mnt/demo-datasets/bookstore/orders`;
describe history orders;

-- COMMAND ----------

-- create or replace table command accept the new table schema, but overwrite command does not accept new schema.

insert overwrite orders
select *,current_timestamp() as current_timestamp from parquet.`dbfs:/mnt/demo-datasets/bookstore/orders`;

-- COMMAND ----------

insert into orders select * from parquet.`dbfs:/mnt/demo-datasets/bookstore/orders-new`

-- COMMAND ----------

describe history orders

-- COMMAND ----------

select * from customers;

-- COMMAND ----------

create or replace temp view customer_update as
select * from json.`dbfs:/mnt/demo-datasets/bookstore/customers-json-new`;



-- COMMAND ----------

merge into customers as c using customer_update as cu
on c.customer_id = cu.customer_id
when matched and c.email is null and cu.email is not null then update set c.email = cu.email, c.updated = cu.updated
when not matched then insert *;


-- COMMAND ----------

create or replace temp view books_update (book_id string,title string, author string, category string, price double)
using csv
options (path = 'dbfs:/mnt/demo-datasets/bookstore/books-csv-new', header=True,delimiter=';');

select * from books_update;


-- COMMAND ----------



-- COMMAND ----------

create table books as select * from books_csv;

-- COMMAND ----------

merge into books as b
using books_update as bu
on b.book_id = bu.book_id and b.title = bu.title
when not matched and bu.category = 'Computer Science' then insert *;


-- COMMAND ----------

describe extended books

-- COMMAND ----------

merge into books as b
using books_update as bu
on b.book_id = bu.book_id and b.title = bu.title
when not matched and bu.category = 'Computer Science' then insert *;
-- if i am running it again, it will not add any duplicate records.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC merge into command can be applied on delta tables only

-- COMMAND ----------

select * from books

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### **advanced transformations**

-- COMMAND ----------

select * from customers 

-- COMMAND ----------


select profile,profile:first_name,profile:address:country from customers


-- COMMAND ----------

select from_json(profile) from customers


-- COMMAND ----------

describe extended customers

-- COMMAND ----------

create or replace temp view parsed_customers as 
select customer_id,from_json(profile,schema_of_json('{"first_name":"Susana","last_name":"Gonnely","gender":"Female","address":{"street":"760 Express Court","city":"Obrenovac","country":"Serbia"}}')) as profile_struct from customers;

select * from parsed_customers;


-- COMMAND ----------

describe extended parsed_customers

-- COMMAND ----------

select *,profile_struct.address.city,profile_struct.gender from parsed_customers


-- COMMAND ----------

create or replace temp view customer_final as 
select customer_id,profile_struct.* from parsed_customers;
select * from customer_final;

-- COMMAND ----------

select * from orders


-- COMMAND ----------

select order_id,customer_id,books.book_id,books,explode(books) as new_books from orders order by customer_id asc,order_id asc;

-- COMMAND ----------

select customer_id,collect_set(order_id) as order_set,collect_set(books.book_id) as bok_id_set from orders group by customer_id;


-- COMMAND ----------

select customer_id,collect_set(books.book_id) as before_flatten,array_distinct(flatten(collect_set(books.book_id))) as after_flatten from orders group by customer_id;



-- COMMAND ----------

create or replace view orders_enriched as 
select * from (
  select *,explode(books) as book from orders)  as a inner join books as b on a.book.book_id = b.book_id;
select * from orders_enriched;

-- COMMAND ----------

create or replace temp view orders_update as 
select * from parquet.`dbfs:/mnt/demo-datasets/bookstore/orders-new`;

select * from orders union select * from orders_update;

-- COMMAND ----------

select * from orders minus select * from orders_update;


-- COMMAND ----------

select * from orders intersect select * from orders_update;


-- COMMAND ----------



-- COMMAND ----------

-- pivot
create or replace table customer_transaction as 
select * from (
  select customer_id,book.book_id as book_id,book.quantity as book_quantity from orders_enriched
) pivot (sum(book_quantity) for book_id in ('B01','B02','B03','B04','B05','B06','B07','B08','B09','B10','B11','B12'));

select * from customer_transaction;

-- COMMAND ----------

-- unpivot

select customer_id,book_id,book_quantity from customer_transaction unpivot (book_quantity for book_id in (B01,B02,B03,B04,B05,B06,B07,B08,B09,B10,B11,B12))
ORDER BY 1,2,3


-- COMMAND ----------

-- MAGIC %md
-- MAGIC **HIGHER ORDER FUNCTIONS AND SQL UDFS**

-- COMMAND ----------

select * from orders

-- COMMAND ----------

select order_id,books,filter(books,i->i.subtotal>=30) as subtotal_30 from orders;

-- COMMAND ----------

select order_id,subtotal_30 from (select order_id,books,filter(books,i->i.subtotal>=30) as subtotal_30 from orders) as t1
where size(t1.subtotal_30) >=3;

-- COMMAND ----------

select order_id,books,transform(books,i->cast(i.subtotal*0.8 as int)) as subtotal_80_discount from orders;

-- COMMAND ----------

create or replace function get_url(email string)
returns string
return concat("https://",split(email,'@')[1])


-- COMMAND ----------

select email,get_url(email) as website from customers

-- COMMAND ----------

describe function get_url

-- COMMAND ----------

describe function extended get_url;

-- COMMAND ----------

create or replace function site_type(email string)
returns string
return case
when email like '%.com' then 'commercial business'
when email like '%.org' then 'non-profit organization'
when email like '%.gov' then 'government agency'
when email like '%.edu' then 'educational institution'
else 'unknown' end


-- COMMAND ----------

select email,site_type(email) as domain from customers

-- COMMAND ----------

select email,value from customers as a 
cross apply string_split(email,'@');

-- COMMAND ----------

select email,explode(split(email,'@')) as new_email from customers
-- cross apply function is not available in databricks

-- COMMAND ----------


