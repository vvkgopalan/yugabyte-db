--
-- Tablegroups
--

-- CREATE DATABASE test_tablegroups

CREATE DATABASE test_tablegroups;
\c test_tablegroups

-- CREATE TABLEGROUP

CREATE TABLEGROUP tg_test1;
CREATE TABLEGROUP tg_test2;

CREATE TABLE tab_nonkey (a INT) TABLEGROUP tg_test1;
\d tab_nonkey
-- Hash partitioned will fail
CREATE TABLE tab_key (a INT PRIMARY KEY) TABLEGROUP tg_test1;
\d tab_key
CREATE TABLE tab_range (a INT, PRIMARY KEY (a ASC)) TABLEGROUP tg_test1;
CREATE TABLE tab_range_nonkey (a INT, b INT, PRIMARY KEY (a ASC)) TABLEGROUP tg_test1;
-- do not use tablegroup
CREATE TABLE tab_nonkey_noco (a INT);
CREATE TABLE tab_range_colo (a INT, PRIMARY KEY (a ASC)) TABLEGROUP tg_test2;

INSERT INTO tab_range (a) VALUES (0), (1), (2);
INSERT INTO tab_range (a, b) VALUES (0, '0'); -- fail
INSERT INTO tab_range_nonkey (a, b) VALUES (0, '0'), (1, '1');
INSERT INTO tab_nonkey_noco (a) VALUES (0), (1), (2), (3);
INSERT INTO tab_range_colo (a) VALUES (0), (1), (2), (3);

SELECT * FROM tab_range;
SELECT * FROM tab_range WHERE a = 2;
SELECT * FROM tab_range WHERE n = '0'; -- fail
SELECT * FROM tab_range_nonkey;
SELECT * FROM tab_nonkey_noco ORDER BY a ASC;
SELECT * FROM tab_range_colo;

BEGIN;
INSERT INTO tab_range_colo VALUES (4);
SELECT * FROM tab_range_colo;
ROLLBACK;
BEGIN;
INSERT INTO tab_range_colo VALUES (5);
COMMIT;
SELECT * FROM tab_range_colo;

INSERT INTO tab_range_colo VALUES (6), (6);

-- CREATE INDEX

-- table with explicit tablegroup for index
CREATE TABLE tab_range_nonkey2 (a INT, b INT, PRIMARY KEY (a ASC)) TABLEGROUP tg_test1;
CREATE INDEX idx_range ON tab_range_nonkey2 (a) TABLEGROUP tg_test1;
\d tab_range_nonkey2
INSERT INTO tab_range_nonkey2 (a, b) VALUES (0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5);
EXPLAIN (COSTS OFF) SELECT * FROM tab_range_nonkey2 WHERE a = 1;
SELECT * FROM tab_range_nonkey2 WHERE a = 1;
UPDATE tab_range_nonkey2 SET b = b + 1 WHERE a > 3;
SELECT * FROM tab_range_nonkey2;
DELETE FROM tab_range_nonkey2 WHERE a > 3;
SELECT * FROM tab_range_nonkey2;

-- table in tablegroup with default index tablegroup
CREATE TABLE tab_range_nonkey3 (a INT, b INT, PRIMARY KEY (a ASC)) TABLEGROUP tg_test1;
CREATE INDEX idx_range_colo ON tab_range_nonkey3 (a);

-- table with no tablegroup (with index having tablegroup)
CREATE TABLE tab_range_nonkey4 (a INT, b INT, PRIMARY KEY (a ASC));
CREATE INDEX idx_range_noco ON tab_range_nonkey4 (a) TABLEGROUP tg_test1;

-- table with index in a different tablegroup
CREATE TABLE tab_range_nonkey_noco (a INT, b INT, PRIMARY KEY (a ASC)) TABLEGROUP tg_test1;
CREATE INDEX idx_range2 ON tab_range_nonkey_noco (a) TABLEGROUP tg_test2;
INSERT INTO tab_range_nonkey_noco (a, b) VALUES (0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5);
EXPLAIN (COSTS OFF) SELECT * FROM tab_range_nonkey_noco WHERE a = 1;
SELECT * FROM tab_range_nonkey_noco WHERE a = 1;
UPDATE tab_range_nonkey_noco SET b = b + 1 WHERE a > 3;
SELECT * FROM tab_range_nonkey_noco;
DELETE FROM tab_range_nonkey_noco WHERE a > 3;
SELECT * FROM tab_range_nonkey_noco;

\dt
\di

-- TRUNCATE TABLE

-- truncate colocated table with default index
TRUNCATE TABLE tab_range;
SELECT * FROM tab_range;
INSERT INTO tab_range VALUES (4);
SELECT * FROM tab_range;
INSERT INTO tab_range VALUES (1);
INSERT INTO tab_range VALUES (2), (5);
SELECT * FROM tab_range;
DELETE FROM tab_range WHERE a = 2;
TRUNCATE TABLE tab_range;
SELECT * FROM tab_range;
INSERT INTO tab_range VALUES (2);
SELECT * FROM tab_range;

TRUNCATE TABLE tab_range;

-- truncate non-colocated table without index
TRUNCATE TABLE tab_nonkey_noco;
SELECT * FROM tab_nonkey_noco;

-- truncate colocated table with explicit index
TRUNCATE TABLE tab_range_nonkey2;
SELECT * FROM tab_range_nonkey2;

\dt
\di

-- ALTER TABLE
INSERT INTO tab_range (a) VALUES (0), (1), (2);
INSERT INTO tab_range_nonkey2 (a, b) VALUES (0, 0), (1, 1);

SELECT * FROM tab_range;
SELECT * FROM tab_range_nonkey2;

-- Alter tablegrouped tables
ALTER TABLE tab_range ADD COLUMN x INT;
ALTER TABLE tab_range_nonkey2 DROP COLUMN b;

SELECT * FROM tab_range;
SELECT * FROM tab_range_nonkey2;

ALTER TABLE tab_range_nonkey2 RENAME TO tab_range_nonkey2_renamed;
SELECT * FROM tab_range_nonkey2_renamed;
SELECT * FROM tab_range_nonkey2;

-- DROP TABLE

-- drop colocated table with default index
DROP TABLE tab_range;
SELECT * FROM tab_range;

-- drop non-colocated table without index
DROP TABLE tab_nonkey_noco;
SELECT * FROM tab_nonkey_noco;

--- drop colocated table with explicit index
DROP TABLE tab_range_nonkey2_renamed;
SELECT * FROM tab_range_nonkey2_renamed;

-- DROP INDEX

-- drop index on colocated table
DROP INDEX idx_range2;
EXPLAIN SELECT * FROM tab_range_nonkey_noco WHERE a = 1;

\dt
\di

-- drop database
\c yugabyte
DROP DATABASE test_tablegroups;
\c test_tablegroups
