DROP SEQUENCE test_seq;
DROP TABLE test PURGE;

CREATE TABLE test AS 
   SELECT o1.*, o1.created fromtime, o1.created totime
   FROM all_objects o1
   WHERE 1=2
/

CREATE SEQUENCE test_seq START WITH 10000 INCREMENT BY 1 CACHE 100
/

INSERT INTO test
   SELECT o1.*, 
       to_date('01-01-2000','dd-mm-yyyy') + trunc(dbms_random.value(1,6700)), 
       null
   FROM all_objects o1, all_objects o2 
   WHERE rownum<=500000;

UPDATE test 
SET object_id = test_seq.nextval, 
    totime = fromtime + trunc(dbms_random.value(1,100));
commit;

CREATE INDEX test_srch01_idx ON test (fromtime, totime, owner);

exec dbms_stats.gather_table_stats(ownname=>'DEVDATA', tabname=>'TEST');

SELECT /* srch01 */ *
FROM test
WHERE fromtime > to_date(:b1,'dd-mm-yyyy')
  AND totime < to_date(:b2,'dd-mm-yyyy')
  AND owner= 'PUBLIC';

SELECT /* srch03_comp */ *
FROM test
WHERE fromtime > to_date(:b1,'dd-mm-yyyy')
  AND totime < to_date(:b2,'dd-mm-yyyy')
  AND fromtime <= to_date(:b3,'dd-mm-yyyy')
  AND owner= 'PUBLIC';

variable b1 varchar2(20)
variable b2 varchar2(20)
exec :b1:='12-03-2017';
exec :b2:='29-03-2017';

SELECT count(*) without_filter, sum(case when totime < to_date(:b2, 'dd-mm-yyyy') then 1 else 0 end) with_filter
FROM test
WHERE fromtime > to_date(:b1,'dd-mm-yyyy')   --'12-03-2017'
  -- totime < to_date(:b2, 'dd-mm-yyyy')     --'29-03-2017'
  AND fromtime < to_date(:b2, 'dd-mm-yyyy')  --'29-03-2017'
  AND owner= 'PUBLIC'; 
  
CREATE INDEX test_srch02_idx ON test (fromtime, owner);
drop index test_srch03_idx;
CREATE INDEX test_srch03_idx ON test (owner, fromtime, totime) compress 1;

alter index test_srch01_idx invisible;
alter index test_srch02_idx invisible;
