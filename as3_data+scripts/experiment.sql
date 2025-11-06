SET PROFILING=1;

-- default query
BEGIN;
select lname, fname from person, history as H where _id=pid and eyear=2000 and H.city='Las Vegas';
ROLLBACK;

-- insert query (from phistory2.tsv) with 1000 records
BEGIN;
load data local infile 'phistory2.tsv' into table history;
ROLLBACK;

-- update query with city
BEGIN;
DELETE FROM history WHERE city="Oslo";
ROLLBACK;


-- delete query with country
BEGIN;
DELETE FROM history WHERE country="Norway";
ROLLBACK;

SET PROFILING=0;

-- delete 100_000 records
BEGIN;
DELETE FROM history LIMIT 100000;
ROLLBACK;
