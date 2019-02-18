
DROP TABLE employee_id_buckets;

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



LOAD DATA LOCAL INPATH 'file:///home/hsmak/Development/git/data-sandbox/hive-sandbox/data/employee_id.txt' OVERWRITE INTO TABLE employee_id_buckets

SELECT * FROM employee_id_buckets;




