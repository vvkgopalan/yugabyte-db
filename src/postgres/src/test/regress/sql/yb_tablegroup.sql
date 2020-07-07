--
-- YB_TABLEGROUP Testsuite: Testing Statments for TABLEGROUP.
--

--
-- pg_catalog alterations. Validate columns of pg_tablegroup and pg_class.
--
\d pg_tablegroup
\d pg_class
--
-- CREATE TABLEGROUP
--
CREATE TABLEGROUP tgroup1;
CREATE TABLEGROUP tgroup2;
CREATE TABLEGROUP tgroup3;
CREATE TABLE tgroup_test1 (col1 int, col2 int) TABLEGROUP tgroup1;
CREATE TABLE tgroup_test2 (col1 int, col2 int) TABLEGROUP tgroup1;
SELECT grpname FROM pg_tablegroup;
SELECT relname FROM pg_class WHERE reltablegroup != 0;
CREATE INDEX ON tgroup_test1(col2) TABLEGROUP tgroup1;
CREATE TABLE tgroup_test3 (col1 int, col2 int) TABLEGROUP tgroup2;
SELECT relname, relhasindex
    FROM pg_tablegroup, pg_class
    WHERE pg_tablegroup.oid = pg_class.reltablegroup;
-- These should fail.
CREATE TABLEGROUP tgroup1;
CREATE TABLE tgroup_test (col1 int, col2 int) TABLEGROUP bad_tgroupname;
CREATE TABLE tgroup_optout (col1 int, col2 int) WITH (colocated=false) TABLEGROUP tgroup1;
CREATE TABLE tgroup_optout (col1 int, col2 int) WITH (colocated=false) TABLEGROUP bad_tgroupname;
CREATE TEMP TABLE tgroup_temp (col1 int, col2 int) TABLEGROUP tgroup1;

--
-- DROP TABLEGROUP
--
DROP TABLEGROUP tgroup3;
-- These should fail.
CREATE TABLE tgroup_test4 (col1 int, col2 int) TABLEGROUP tgroup3;
DROP TABLEGROUP tgroup1;
DROP TABLEGROUP bad_tgroupname;
-- This drop should work now.
DROP TABLE tgroup_test1;
DROP TABLE tgroup_test2;
DROP TABLEGROUP tgroup1;
-- Create a tablegroup with the name of a dropped tablegroup.
CREATE TABLEGROUP tgroup1; 
--
-- Interactions with colocated database.
--
CREATE DATABASE db_colocated colocated=true;
\c db_colocated
-- These should fail.
CREATE TABLEGROUP tgroup1;
CREATE TABLE tgroup_test (col1 int, col2 int) TABLEGROUP tgroup1;
CREATE TABLE tgroup_optout (col1 int, col2 int) WITH (colocated=false) TABLEGROUP tgroup1;
