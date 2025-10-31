CREATE KEYSPACE sya236 WITH REPLICATION = {
'class': 'SimpleStrategy', 'replication_factor': 2 };

USE sya236;

CREATE TABLE test ( id int PRIMARY KEY, data text );
INSERT INTO test (id, data) VALUES (1, 'initial');
INSERT INTO test (id, data) VALUES (2, 'secondary');
INSERT INTO test (id, data) VALUES (3, 'third');
UPDATE test SET data='tertiary' WHERE id=3;
SELECT * FROM test;

INSERT INTO test (id, data) VALUES (2, 'double');
SELECT * FROM test;

SELECT * FROM test WHERE data='initial';
SELECT * FROM test WHERE data='initial' ALLOW FILTERING;

CREATE INDEX data_index ON test (data);
SELECT * FROM test WHERE data='initial';

BEGIN BATCH
  INSERT INTO test (id, data) VALUES (4, 'square');
  INSERT INTO test (id, data) VALUES (5, 'cinq');
APPLY BATCH;

ALTER KEYSPACE sya236 WITH REPLICATION = {
'class': 'SimpleStrategy', 'replication_factor': 1 };

USE sya236;

CREATE TABLE IF NOT EXISTS t (id int PRIMARY KEY, data text);

BEGIN BATCH
  INSERT INTO t (id,data) VALUES (1,'a');
  INSERT INTO t (id,data) VALUES (2,'b');
  INSERT INTO t (id,data) VALUES (3,'c');
  INSERT INTO t (id,data) VALUES (10,'x');
  INSERT INTO t (id,data) VALUES (20,'y');
  INSERT INTO t (id,data) VALUES (30,'z');
  INSERT INTO t (id,data) VALUES (100,'m');
  INSERT INTO t (id,data) VALUES (200,'n');
  INSERT INTO t (id,data) VALUES (300,'p');
APPLY BATCH;
-- 	1.	unreliable3 在线窗口（00–10、20–30、40–50 分）逐个查：
CONSISTENCY ONE;
SELECT * FROM t WHERE id IN (1,2,3,10,20,30,100,200,300);
-- 	2.	unreliable3 离线窗口（10–20、30–40、50–60 分）逐个查：
SELECT * FROM t WHERE id IN (1,2,3,10,20,30,100,200,300);

ALTER KEYSPACE sya236 WITH REPLICATION = {
'class': 'SimpleStrategy', 'replication_factor': 2 };
CONSISTENCY ONE;
INSERT INTO test (id, data) VALUES (6, 'hexadecimal');
SELECT * FROM test;
CONSISTENCY ALL;
INSERT INTO test (id, data) VALUES (7, 'sevenish');
INSERT INTO test (id, data) VALUES (8, 'octagonal');
INSERT INTO test (id, data) VALUES (9, 'neun');
SELECT * FROM test;
SELECT * FROM test WHERE id=1;
SELECT * FROM test WHERE id=2;
SELECT * FROM test WHERE id=3;
SELECT * FROM test WHERE id=4;
SELECT * FROM test WHERE id=5;

-- on reliable cluster, after USE sya236;
CREATE TABLE nasalogs (
  host     text,
  datetime timestamp,
  path     text,
  bytes    int,
  req_id   timeuuid,
  PRIMARY KEY ((host), datetime, req_id)
) WITH CLUSTERING ORDER BY (datetime ASC);

SELECT sum(bytes) From nasalogs
-- If you'd like to clear the data in your table in between runs, you can use this statement in CQLSH:
-- TRUNCATE nasalogs;

-- without partition
-- real    5m51.234s
-- user    0m36.227s
-- sys     0m4.455s
--  with partition
--  rdd = rdd.keyBy(lambda t: t[0]).repartition(8).values()