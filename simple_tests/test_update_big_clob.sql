drop table test;
drop table test2;

create table test as select a.* from all_objects a, all_objects b where rownum<=100000;

create table test2 (id number, my_data clob);

insert into test2 values (1,null);


DECLARE
   l_clob clob;
BEGIN
   for rec in (select owner || ';' || object_name || ';' || object_type || ';' || to_char(last_ddl_time, 'DD.MM.YYYY HH24:MI:SS') || chr(10) my_data
               from test 
               where rownum<=100000) loop
      l_clob := l_clob || rec.my_data;
   end loop;
   
   update test2 set my_data = l_clob where id=1;
   --insert into test2(id, my_data) values (1, l_clob); --returning my_data into l_clob;
   
END;
/

select * from test2;