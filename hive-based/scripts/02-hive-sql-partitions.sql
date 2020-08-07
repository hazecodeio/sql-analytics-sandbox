
DROP TABLE IF EXISTS employee_partitioned;

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

--Check partition table structure
DESC employee_partitioned



LOAD DATA LOCAL INPATH 'file:///home/hsmak/Development/git/data-sandbox/hive-based/data/employee.txt'
    OVERWRITE INTO TABLE Employee_Partitioned
    PARTITION (year=2018, month=12);


SELECT * from Employee;


--Verify data loaded
SELECT name, year, month FROM employee_partitioned;

SELECT name, year, month FROM employee_partitioned WHERE year=2018;




--Show partitions
SHOW PARTITIONS employee_partitioned

--Add multiple partitions (Static)
ALTER TABLE employee_partitioned ADD
    PARTITION (year=2018, month=11)
    PARTITION (year=2018, month=12);

SHOW PARTITIONS employee_partitioned;

--Drop partitions
ALTER TABLE employee_partitioned DROP PARTITION (year=2018, month=11);

ALTER TABLE employee_partitioned DROP IF EXISTS PARTITION (year=2017); -- Drop all partitions in 2017

ALTER TABLE employee_partitioned DROP IF EXISTS PARTITION (month=9);

SHOW PARTITIONS employee_partitioned