--
-- Tests for altering tablegroups. Includes name / owner changes.
-- Will eventually include tests on pulling tables out of tablegroups / altering the tablegroup of a table etc.
--

-- Test rename
CREATE TABLEGROUP tgroup1;
CREATE TABLEGROUP tgroup2;

SELECT grpname FROM pg_tablegroup;
ALTER TABLEGROUP tgroup1 RENAME TO tgroup_alt;
SELECT grpname FROM pg_tablegroup;

ALTER TABLEGROUP tgroup2 RENAME TO tgroup_alt; -- fail
ALTER TABLEGROUP tgroup2 RENAME TO tgroup2; -- fail
SELECT grpname FROM pg_tablegroup;
ALTER TABLEGROUP tgroup_not_exists RENAME TO tgroup_not_exists; -- fail

-- Test alter owner
CREATE USER u1;
CREATE USER u2;
CREATE TABLEGROUP tgroup3 OWNER u1;
CREATE TABLEGROUP tgroup4 OWNER u1;
CREATE TABLEGROUP tgroup5;

ALTER TABLEGROUP tgroup3 OWNER TO u2;
ALTER TABLEGROUP tgroup4 OWNER TO u3; -- fail
ALTER TABLEGROUP tgroup_not_exists OWNER TO u1; -- fail

\c yugabyte u1
DROP TABLEGROUP tgroup3; -- fail
DROP TABLEGROUP tgroup4;
ALTER TABLEGROUP tgroup5 OWNER TO u1; -- fail

\c yugabyte u2
DROP TABLEGROUP tgroup3;
