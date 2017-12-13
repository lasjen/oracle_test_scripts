-- This smal test is copied from Martin Widlake's blog
-- https://mwidlake.wordpress.com/2010/06/21/dbms_stats-set_table_stats-defaults/

drop table TEST1 purge
/

select sysdate from dual
/

CREATE TABLE TEST1 (
  ID number,
  OBJ_NAME varchar2(50)
)
/
  
INSERT INTO TEST1 
SELECT      ROWNUM ID, OBJECT_NAME OBJ_NAME
FROM USER_OBJECTS
/

SELECT TABLE_NAME,NUM_ROWS,BLOCKS,SAMPLE_SIZE
FROM ALL_TABLES
WHERE OWNER=USER AND TABLE_NAME = 'TEST1'
/

EXEC dbms_stats.set_table_stats(ownname => user,tabname => 'TEST1')
/

SELECT TABLE_NAME,NUM_ROWS,BLOCKS,SAMPLE_SIZE
FROM ALL_TABLES
WHERE OWNER=USER AND TABLE_NAME = 'TEST1'
/

EXEC dbms_stats.set_table_stats(ownname => user,tabname => 'TEST1',numrows=>5000, NUMBLKS=>500)
/

EXEC dbms_stats.set_table_stats(ownname => user,tabname => 'TEST1',NUMBLKS=>500)
/

desc dbms_stats
