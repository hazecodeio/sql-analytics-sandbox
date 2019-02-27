
DROP TABLE IF EXISTS employee_contract;

--Prepare table and data for demonstration
CREATE TABLE IF NOT EXISTS employee_contract(
    name string,
    dept_num int,
    employee_id int,
    salary int,
    type string,
    start_date date
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH 'file:///home/hsmak/Development/git/data-sandbox/hive-sandbox/data/employee_contract.txt' OVERWRITE INTO TABLE employee_contract

SELECT * FROM employee_contract;


-- Windowing => Partition + Ordering

-- 'PARTITION BY' - to show accumulative values from previous row. Accumulative per partition; partition

SELECT employee_id, name, dept_num , salary, type, start_date,

  row_number() OVER() as nonpartitioned_nonordered_row_n,
  row_number() OVER(ORDER BY employee_id) as nonpartitioned_row_n,
  row_number() OVER(PARTITION BY dept_num ORDER BY employee_id) as partitioned_row_n,
  salary,
  sum(salary) OVER (PARTITION BY dept_num ORDER BY employee_id) as total_salary,
  sum(salary) OVER (PARTITION BY dept_num ORDER BY employee_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as total_salary_2,
  sum(salary) OVER (PARTITION BY dept_num ORDER BY employee_id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as total_salary_3

  FROM employee_contract;










--window aggregate functions
SELECT name, dept_num as deptno, salary,

  count(*)                  OVER (PARTITION BY dept_num) as cnt,
  count(distinct dept_num)  OVER (PARTITION BY dept_num) as dcnt,
  sum(salary)               OVER (PARTITION BY dept_num ORDER BY dept_num) as sum1,
  sum(salary)               OVER (ORDER BY dept_num) as sum2,
  sum(salary)               OVER (ORDER BY dept_num, name) as sum3

  FROM employee_contract ORDER BY deptno, name;

--window sorting functions
SELECT name, dept_num as deptno, salary,

  row_number()    OVER () as rnum,
  rank()          OVER (PARTITION BY dept_num ORDER BY salary) as rk,
  dense_rank()    OVER (PARTITION BY dept_num ORDER BY salary) as drk,
  percent_rank()  OVER (PARTITION BY dept_num ORDER BY salary) as prk,
  ntile(4)        OVER (PARTITION BY dept_num ORDER BY salary) as ntile_4,
  ntile(3)        OVER (PARTITION BY dept_num ORDER BY salary) as ntile_3,
  ntile(2)        OVER (PARTITION BY dept_num ORDER BY salary) as ntile_2

  FROM employee_contract ORDER BY deptno, name;

--aggregate in over clause
SELECT dept_num,

  rank()    OVER (PARTITION BY dept_num ORDER BY sum(salary)) as rk

  FROM employee_contract GROUP BY dept_num;




--window analytics function
SELECT name, dept_num as deptno, salary,

  round(cume_dist()   OVER (PARTITION BY dept_num ORDER BY salary), 2) as cume,
  lead(salary, 2)     OVER (PARTITION BY dept_num ORDER BY salary) as lead,
  lag(salary, 2, 0)   OVER (PARTITION BY dept_num ORDER BY salary) as lag,
  first_value(salary) OVER (PARTITION BY dept_num ORDER BY salary) as fval,
  last_value(salary)  OVER (PARTITION BY dept_num ORDER BY salary) as lvalue,
  last_value(salary)  OVER (PARTITION BY dept_num ORDER BY salary RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lvalue2

  FROM employee_contract ORDER BY deptno, salary;

--window expression preceding and following
SELECT name, dept_num as dno, salary AS sal,

  max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) win1,
  max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS BETWEEN 2 PRECEDING AND UNBOUNDED FOLLOWING) win2,
  max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING) win3,
  max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) win4,
  max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) win5,
  max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS 2 PRECEDING) win6,
  max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS UNBOUNDED PRECEDING) win7

  FROM employee_contract ORDER BY dno, name;

--window expression current_row
SELECT name, dept_num as dno, salary AS sal,

  max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS BETWEEN CURRENT ROW AND CURRENT ROW) win8,
  max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) win9,
  max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) win10,
  max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) win11,
  max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) win12,
  max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) win13,
  max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) win14

  FROM employee_contract ORDER BY dno, name;

--window reference
SELECT name, dept_num, salary,

  MAX(salary) OVER w1 AS win1,
  MAX(salary) OVER w2 AS win2,
  MAX(salary) OVER w3 AS win3

  FROM employee_contract WINDOW
      w1 as (PARTITION BY dept_num ORDER BY name ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),
      w2 as w3,
      w3 as (PARTITION BY dept_num ORDER BY name ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING)
;

--window with range type
SELECT dept_num, start_date, name, salary,

  max(salary) OVER (PARTITION BY dept_num ORDER BY salary RANGE BETWEEN 500 PRECEDING AND 1000 FOLLOWING) win1,
  max(salary) OVER (PARTITION BY dept_num ORDER BY salary RANGE BETWEEN 500 PRECEDING AND CURRENT ROW) win2

  FROM employee_contract order by dept_num, start_date;





--random sampling
SELECT name FROM employee_hr DISTRIBUTE BY rand() SORT BY rand() LIMIT 2;

--Bucket table sampling example
--based on whole row
SELECT name FROM employee_trans TABLESAMPLE(BUCKET 1 OUT OF 2 ON rand()) a;
--based on bucket column
SELECT name FROM employee_trans TABLESAMPLE(BUCKET 1 OUT OF 2 ON emp_id) a;

--Block sampling - Sample by rows
SELECT name FROM employee TABLESAMPLE(1 ROWS) a;

--Sample by percentage of data size
SELECT name FROM employee TABLESAMPLE(50 PERCENT) a;

--Sample by data size
SELECT name FROM employee TABLESAMPLE(3b) a;