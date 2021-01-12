-- ############################################ --

-- -- Create a Keyspace --
DROP KEYSPACE if exists KSOfHusain;
CREATE KEYSPACE if not exists KSOfHusain WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};

USE KSOfHusain;

--
DROP TABLE IF EXISTS reversed_user_status_updates;
CREATE TABLE reversed_user_status_updates
(
    username text,
    id       timeuuid,
    body     text,
    PRIMARY KEY (username, id)
) WITH CLUSTERING ORDER BY (id DESC);

INSERT INTO reversed_user_status_updates(username, id, body) VALUES ('alice', NOW(), 'Reversed status 1');
INSERT INTO reversed_user_status_updates(username, id, body) VALUES ('alice', NOW(), 'Reversed status 2');
INSERT INTO reversed_user_status_updates(username, id, body) VALUES ('alice', NOW(), 'Reversed status 3');
SELECT * FROM reversed_user_status_updates WHERE username = 'alice';
SELECT * FROM reversed_user_status_updates WHERE username = 'alice' ORDER BY id ASC;

--