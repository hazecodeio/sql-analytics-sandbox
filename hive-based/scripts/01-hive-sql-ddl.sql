
-- Creating a simple table in Hive

CREATE TABLE Employee(
    EmployeeID  INT,
    FirstName   VARCHAR(30),
    LastName    VARCHAR(30),
    Salary      DOUBLE
);

DESCRIBE Employee;
DESCRIBE EXTENDED Employee;

---------------------------------------
-- Creating Tables
-- Loading with data
-- Querying Fields of: Collection, Map, Struct, Map of Collections, etc
---------------------------------------

DROP TABLE Employee;

CREATE TABLE employee (
    name          STRING,
    work_place    ARRAY<STRING>,
    gender_age    STRUCT<gender:STRING,age:INT>,
    skills_score  MAP<STRING,INT>,
    depart_title  MAP<STRING,ARRAY<STRING>>
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':'
STORED AS TEXTFILE;

DESCRIBE Employee;
DESCRIBE EXTENDED Employee;


--------------------- LOAD data from LOCAL into the a Hive table ------------------------

/*LOCAL means from local fileSystem. W/out LOCAL means within HDFS*/
LOAD DATA LOCAL INPATH 'file:///home/hsmak/Development/git/data-sandbox/hive-based/data/employee.txt' OVERWRITE INTO TABLE Employee

SELECT * from Employee






-- Query the whole array and each array element in the table:
SELECT work_place from employee
SELECT work_place[0] as col_1, work_place[1] as col_2, work_place[2] as col_3 from employee

-- Query the whole struct and each struct attribute in the table:
SELECT gender_age FROM employee
SELECT gender_age.gender, gender_age.age FROM employee


-- Query the whole map and each map element in the table:
SELECT skills_score from employee
SELECT  name,
        skills_score['DB']      as DB,
        skills_score['Perl']    as Perl,
        skills_score['Python']  as Python,
        skills_score['Sales']   as Sales,
        skills_score['HR']      as HR
        FROM employee

-- Query the composite type in the table:
SELECT depart_title FROM employee;
SELECT  name,
        depart_title['Product'] as Product,
        depart_title['Test']    as Test,
        depart_title['COE']     as COE,
        depart_title['Sales']   as Sales
        FROM employee;




---------------------- CREATING/DROPPING DB/Schema ------------------------

--Create database without checking if the database already exists.
CREATE DATABASE MyHiveDB

--Create database and checking if the database already exists.
CREATE DATABASE IF NOT EXISTS MyHiveDB

--Create database with location, comments, and metadata information
CREATE DATABASE IF NOT EXISTS MyHiveDB
  COMMENT 'hive database demo'
  LOCATION '/hdfs/directory'
  WITH DBPROPERTIES ('creator'='dayongd','date'='2018-01-01');

--Show and describe database with wildcards
SHOW DATABASES
SHOW DATABASES LIKE 'my.*'
DESCRIBE DATABASE default
DESCRIBE DATABASE MyHiveDB

--Use the database
USE MyHiveDB

--Show current database
SELECT current_database();
SHOW TABLES


--metadata about database could not be changed.
ALTER DATABASE MyHiveDB SET DBPROPERTIES ('edited-by' = 'Husain')

ALTER DATABASE MyHiveDB SET OWNER user Husain

--Drop the empty database.
DROP DATABASE IF EXISTS MyHiveDB

--Drop database with CASCADE
DROP DATABASE IF EXISTS MyHiveDB CASCADE







-- Hive View DDL

--Create Hive view
CREATE VIEW employee_skills
AS
SELECT name, skills_score['DB'] AS DB,
skills_score['Perl'] AS Perl, skills_score['Python'] AS Python,
skills_score['Sales'] as Sales, skills_score['HR'] as HR
FROM employee;

--Show views
SHOW VIEWS
SHOW VIEWS 'employee_*'
DESC FORMATTED employee_skills
SHOW CREATE TABLE employee_skills

SELECT * FROM employee_skills;

