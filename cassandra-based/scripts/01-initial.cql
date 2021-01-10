-- ############################################ --
use system_schema;
select * from system_schema.keyspaces;
SELECT cluster_name, listen_address FROM system.local;

-- -- Create a Keyspace --
DROP KEYSPACE if exists KSOfHusain;
CREATE KEYSPACE if not exists KSOfHusain WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};

USE KSOfHusain;
--
DROP TABLE IF EXISTS emp;
CREATE TABLE if not exists emp(
    emp_id int PRIMARY KEY,
    emp_name text,
    emp_city text,
    emp_sal varint,
    emp_phone varint);
SELECT * FROM emp;

--
ALTER TABLE emp ADD emp_email text;
SELECT * FROM emp;

--
ALTER TABLE emp DROP emp_email;
SELECT * FROM emp;

--
INSERT INTO emp(emp_id, emp_name, emp_city, emp_sal, emp_phone) VALUES (1, 'Husain', 'Chicago', 155000, 123456789);
SELECT * FROM emp;
UPDATE emp SET emp_sal = 165999, emp_phone=1112223456 WHERE emp_id =1;
SELECT * FROM emp;
DELETE FROM emp WHERE emp_id = 1;
SELECT * FROM emp;

--
CREATE INDEX empIndex ON emp (emp_name);
DROP INDEX empindex;

-- Collection as a type: list<t> | set<t> | map<kt1,vt2>
ALTER TABLE emp ADD emp_email_list list<text>;
INSERT INTO emp(emp_id, emp_name, emp_city, emp_sal, emp_phone, emp_email_list) VALUES (2, 'Husain2', 'Raleigh', 155000, 123456789, ['11@11.com','22@22.com']);
SELECT * FROM emp;

ALTER TABLE emp DROP emp_email_set;
ALTER TABLE emp ADD emp_email_set set<text>;
INSERT INTO emp(emp_id, emp_name, emp_city, emp_sal, emp_phone, emp_email_set) VALUES (3, 'Husain3', 'xyz', 155000, 123456789, {'11@11.com','22@22.com'});
SELECT * FROM emp;

ALTER TABLE emp DROP emp_email_map;
ALTER TABLE emp ADD emp_email_map map<text,text>;
INSERT INTO emp(emp_id, emp_name, emp_city, emp_sal, emp_phone, emp_email_map) VALUES (4, 'Husain3', 'xyz', 155000, 123456789, {'work':'11@11.com','personal':'22@22.com'});
SELECT * FROM emp;

--
DROP TYPE IF EXISTS card_details;
CREATE TYPE card_details (
    num int,
    pin int,
    name text,
    cvv int,
    phone set<int>);

ALTER TYPE card_details ADD email text;
ALTER TYPE card_details RENAME email TO mail;
select * from system_schema.types where keyspace_name = 'ksofhusain';
select * from system_schema.tables where keyspace_name = 'ksofhusain';
