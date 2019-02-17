CREATE TABLE Employee(
    EmployeeID  INT,
    FirstName   VARCHAR(30),
    LastName    VARCHAR(30),
    Salary      DOUBLE
);

DESCRIBE Employee;
DESCRIBE EXTENDED Employee;

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


-- Load data into the table:

/*LOCAL means from local fileSystem. W/out LOCAL means within HDFS*/
LOAD DATA LOCAL INPATH 'file:///home/hsmak/Development/git/data-sandbox/hive-sandbox/employee.txt' OVERWRITE INTO TABLE Employee

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
        skills_score['DB'] as DB,
        skills_score['Perl'] as Perl,
        skills_score['Python'] as Python,
        skills_score['Sales'] as Sales,
        skills_score['HR'] as HR
        FROM employee

-- Query the composite type in the table:
SELECT depart_title FROM employee;
SELECT  name,
        depart_title['Product'] as Product,
        depart_title['Test']    as Test,
        depart_title['COE']     as COE,
        depart_title['Sales']   as Sales
        FROM employee;




-- Partitions
CREATE TABLE employee_partitioned (
    name STRING,
    work_place ARRAY<STRING>,
    gender_age STRUCT<gender:STRING,age:INT>,
    skills_score MAP<STRING,INT>,
    depart_title MAP<STRING,ARRAY<STRING>>
)
PARTITIONED BY (year INT, month INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':';

SHOW PARTITIONS employee_partitioned



--Create the buckets table
CREATE TABLE employee_id_buckets (
    name STRING,
    employee_id INT,  -- Use this table column as bucket column later
    work_place ARRAY<STRING>,
    gender_age STRUCT<gender:string,age:int>,
    skills_score MAP<string,int>,
    depart_title MAP<string,ARRAY<string >>
)
CLUSTERED BY (employee_id) INTO 2 BUCKETS -- Support more columns
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':';