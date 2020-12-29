-- ############################################ --
-- copied from Spark Windowing Tutorial: https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-window.html

show databases;
create database scaladb;
use scaladb;

DROP TABLE IF EXISTS employees;
CREATE TABLE employees (name VARCHAR(255), dept VARCHAR(255), salary INT, age INT);

INSERT INTO employees VALUES ('Lisa', 'Sales', 10000, 35);
INSERT INTO employees VALUES ('Evan', 'Sales', 32000, 38);
INSERT INTO employees VALUES ('Fred', 'Engineering', 21000, 28);
INSERT INTO employees VALUES ('Alex', 'Sales', 30000, 33);
INSERT INTO employees VALUES ('Tom', 'Engineering', 23000, 33);
INSERT INTO employees VALUES ('Jane', 'Marketing', 29000, 28);
INSERT INTO employees VALUES ('Jeff', 'Marketing', 35000, 38);
INSERT INTO employees VALUES ('Paul', 'Engineering', 29000, 23);
INSERT INTO employees VALUES ('Chloe', 'Engineering', 23000, 25);
INSERT INTO employees VALUES ('Helen', 'Marketing', 29000, 40);


SELECT * FROM employees;

SELECT
    name,
    dept,
    RANK() over(PARTITION BY dept ORDER BY salary) as rnk
FROM employees;

SELECT
    name,
    dept,
    salary,
    DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS dns_rnk
FROM employees;

SELECT
    name,
    dept,
    age,
    CUME_DIST() OVER (PARTITION BY dept ORDER BY age RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cume_dst
FROM employees;

SELECT
    name,
    dept,
    salary,
    MIN(salary) OVER (PARTITION BY dept ORDER BY salary) AS minimum
FROM employees;

SELECT
    name,
    dept,
    salary,
    LAG(salary) OVER (PARTITION BY dept ORDER BY salary) AS one_salary_below,
    LEAD(salary, 1, 0) OVER (PARTITION BY dept ORDER BY salary) AS one_salary_above
FROM employees;