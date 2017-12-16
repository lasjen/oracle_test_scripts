-- ----------------------------------------------------------------------
-- Script: test_flashback_table.sql
-- Description:
-- Creating a table with 100000 rows (big_table script from Tom Kyte).
-- Enabling Row Movement. Deleting 50000 rows. Flashing back afterwards.
-- ----------------------------------------------------------------------
drop table big_table purge
/

create table big_table
as
select rownum id,
               OWNER, OBJECT_NAME, SUBOBJECT_NAME,
               OBJECT_ID, DATA_OBJECT_ID,
               OBJECT_TYPE, CREATED, LAST_DDL_TIME,
               TIMESTAMP, STATUS, TEMPORARY,
               GENERATED, SECONDARY
  from all_objects a
 where 1=0
/
alter table big_table nologging;

declare
    l_cnt number;
    l_rows number := 100000;
begin
    insert /*+ append */
    into big_table
    select rownum,
               OWNER, OBJECT_NAME, SUBOBJECT_NAME,
               OBJECT_ID, DATA_OBJECT_ID,
               OBJECT_TYPE, CREATED, LAST_DDL_TIME,
               TIMESTAMP, STATUS, TEMPORARY,
               GENERATED, SECONDARY
      from all_objects a
     where rownum <= &1;

    l_cnt := sql%rowcount;

    commit;

    while (l_cnt < l_rows)
    loop
        insert /*+ APPEND */ into big_table
        select rownum+l_cnt,
               OWNER, OBJECT_NAME, SUBOBJECT_NAME,
               OBJECT_ID, DATA_OBJECT_ID,
               OBJECT_TYPE, CREATED, LAST_DDL_TIME,
               TIMESTAMP, STATUS, TEMPORARY,
               GENERATED, SECONDARY
          from big_table
         where rownum <= l_rows-l_cnt;
        l_cnt := l_cnt + sql%rowcount;
        commit;
    end loop;
end;
/

alter table big_table add constraint
big_table_pk primary key(id)
/

begin
   dbms_stats.gather_table_stats
   ( ownname    => user,
     tabname    => 'BIG_TABLE',
     cascade    => TRUE );
end;
/
select count(*) from big_table;

variable my_scn number

set serveroutput on
begin
  select dbms_flashback.get_system_change_number into :my_scn from dual; --144831897008
  
  dbms_output.put_line('SCN: ' || :my_scn);
end;
/

ALTER TABLE big_table ENABLE ROW MOVEMENT;

delete from big_table where rownum<=50000;
commit;

begin
  FLASHBACK TABLE big_table TO SCN :my_scn; 
end;
/