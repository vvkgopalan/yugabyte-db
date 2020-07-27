--
-- Tablegroups
--

-- CREATE DATABASE test_tablegroups

CREATE DATABASE test_tablegroups;
\c test_tablegroups

-- CREATE TABLEGROUP

CREATE TABLEGROUP tg_test1;
CREATE TABLEGROUP tg_test2;

-- CREATE TABLE
-- No primary key
CREATE TABLE tab_nonkey (a INT) TABLEGROUP tg_test1;
\d tab_nonkey
-- Hash partitioned will fail
CREATE TABLE tab_key (a INT PRIMARY KEY) TABLEGROUP tg_test1;
\d tab_key
-- Range primary key
CREATE TABLE tab_range (a INT, PRIMARY KEY (a ASC)) TABLEGROUP tg_test1;
CREATE TABLE tab_range_multicol (a INT, b INT, PRIMARY KEY (a ASC, b DESC)) TABLEGROUP tg_test1;
-- do not use tablegroup
CREATE TABLE tab_nonkey_nogrp (a INT);
CREATE TABLE tab_hash_nogrp (a INT PRIMARY KEY);

-- INSERT
INSERT INTO tab_range (a) VALUES (0), (1), (2);
INSERT INTO tab_range (a, b) VALUES (0, '0'); -- fail
INSERT INTO tab_range_multicol (a, b) VALUES (0, '0'), (1, '1');
INSERT INTO tab_nonkey_nogrp (a) VALUES (0), (1), (2), (3);
INSERT INTO tab_hash_nogrp (a) VALUES (0), (1), (2), (3);

-- SELECT
SELECT * FROM tab_range;
SELECT * FROM tab_range WHERE a = 2;
SELECT * FROM tab_range WHERE n = '0'; -- fail
SELECT * FROM tab_range_multicol;
SELECT * FROM tab_nonkey_nogrp ORDER BY a ASC;
SELECT * FROM tab_hash_nogrp;

BEGIN;
INSERT INTO tab_range VALUES (4);
SELECT * FROM tab_range;
ROLLBACK;
BEGIN;
INSERT INTO tab_range VALUES (5);
COMMIT;
SELECT * FROM tab_range;

INSERT INTO tab_range VALUES (6), (6); -- fail

-- CREATE INDEX

-- table with index in the tablegroup
CREATE TABLE tab_range2 (a INT, b INT);
CREATE INDEX idx_range2 ON tab_range2 (a);
\d tab_range2
INSERT INTO tab_range2 (a, b) VALUES (0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5);
EXPLAIN (COSTS OFF) SELECT * FROM tab_range2 WHERE a = 1;
SELECT * FROM tab_range2 WHERE a = 1;
UPDATE tab_range2 SET b = b + 1 WHERE a > 3;
SELECT * FROM tab_range2 ORDER BY a;
DELETE FROM tab_range2 WHERE a > 3;
SELECT * FROM tab_range2 ORDER BY a;

-- table with no tablegroup with index
CREATE TABLE tab_range_nogrp (a INT, b INT, PRIMARY KEY (a ASC));
CREATE INDEX idx_range_nogrp ON tab_range_nogrp (a);

-- table with index in a different tablegroup
CREATE TABLE tab_range_nonkey2 (a INT, b INT) TABLEGROUP tg_test2;
CREATE INDEX idx_range_nonkey2 ON tab_range_nonkey2 (a);
INSERT INTO tab_range_nonkey2 (a, b) VALUES (0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5);
EXPLAIN (COSTS OFF) SELECT * FROM tab_range_nonkey2 WHERE a = 1;
SELECT * FROM tab_range_nonkey2 WHERE a = 1;
UPDATE tab_range_nonkey2 SET b = b + 1 WHERE a > 3;
SELECT * FROM tab_range_nonkey2 ORDER BY a;
DELETE FROM tab_range_nonkey2 WHERE a > 3;
SELECT * FROM tab_range_nonkey2 ORDER BY a;

\dt
\di

-- TRUNCATE TABLE

-- truncate tablegroup table with default index
TRUNCATE TABLE tab_range;
SELECT * FROM tab_range;
INSERT INTO tab_range VALUES (4);
SELECT * FROM tab_range;
INSERT INTO tab_range VALUES (1);
INSERT INTO tab_range VALUES (2), (5);
SELECT * FROM tab_range;
DELETE FROM tab_range WHERE a = 2;
SELECT * FROM tab_range;

TRUNCATE TABLE tab_range;

-- truncate non-colocated table without index
TRUNCATE TABLE tab_nonkey_nogrp;
SELECT * FROM tab_nonkey_nogrp;

-- truncate colocated table with explicit index
TRUNCATE TABLE tab_range_nonkey2;
SELECT * FROM tab_range_nonkey2;

\dt
\di

-- ALTER TABLE
INSERT INTO tab_range (a) VALUES (0), (1), (2);
INSERT INTO tab_range_nonkey2 (a, b) VALUES (0, 0), (1, 1);

SELECT * FROM tab_range;
SELECT * FROM tab_range_nonkey2 ORDER BY a;

-- Alter tablegrouped tables
ALTER TABLE tab_range ADD COLUMN x INT;
ALTER TABLE tab_range_nonkey2 DROP COLUMN b;

SELECT * FROM tab_range;
SELECT * FROM tab_range_nonkey2 ORDER BY a;

ALTER TABLE tab_range_nonkey2 RENAME TO tab_range_nonkey2_renamed;
SELECT * FROM tab_range_nonkey2_renamed ORDER BY a;
SELECT * FROM tab_range_nonkey2;

-- DROP TABLE

-- drop table that is in a tablegroup with default index
DROP TABLE tab_range;
SELECT * FROM tab_range;

-- drop non-colocated table without index
DROP TABLE tab_nonkey_nogrp;
SELECT * FROM tab_nonkey_nogrp;

--- drop colocated table with explicit index
DROP TABLE tab_range_nonkey2_renamed;
SELECT * FROM tab_range_nonkey2_renamed;

-- DROP INDEX

-- drop index on colocated table
DROP INDEX idx_range2;
EXPLAIN SELECT * FROM tab_range2 WHERE a = 1;

\dt
\di

-- DROP TABLEGROUP
DROP TABLEGROUP tg_test1; -- fail
DROP TABLEGROUP tg_test2;

-- drop database
\c yugabyte
DROP DATABASE test_tablegroups;
\c test_tablegroups
