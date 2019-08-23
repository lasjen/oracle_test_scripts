drop table test;
drop table test2;

create table test as select a.* from all_objects a, all_objects b where rownum<=1000000;

create table test2 (id number, my_data clob);

------------------------------------------
-- Update with bind
------------------------------------------
insert into test2 values (1,null);
alter session set tracefile_identifier='LJ05';
alter session set events '10046 trace name context forever, level 12';
DECLARE
   l_clob clob;
BEGIN
   l_clob := 'owner;object_name;object_type;last_ddl_time' || chr(10);
   for rec in (select owner || ';' || object_name || ';' || object_type || ';' || to_char(last_ddl_time, 'DD.MM.YYYY HH24:MI:SS') || chr(10) my_data
               from test 
               where rownum<=1000000) loop
      l_clob := l_clob || rec.my_data;
   end loop;
   
   update test2 set my_data = l_clob where id=1;
   --insert into test2(id, my_data) values (1, l_clob); --returning my_data into l_clob;
   
END;
/
alter session set events '10046 trace name context off';

------------------------------------------
-- Update with bulk
------------------------------------------
insert into test2 values (1,null);
alter session set tracefile_identifier='UPDBULK01';
alter session set events '10046 trace name context forever, level 12';
DECLARE
   l_clob clob;
   
   type vc100_nt is table of varchar2(100);
   l_rows vc100_nt;
   
   cursor c_rows is 
      select owner || ';' || object_name || ';' || object_type || ';' || to_char(last_ddl_time, 'DD.MM.YYYY HH24:MI:SS') || chr(10) my_data
      from test where rownum<=1000000;
BEGIN
   l_clob := 'owner;object_name;object_type;last_ddl_time' || chr(10);
   
   open c_rows;
   loop
      fetch c_rows bulk collect into l_rows limit 1000;
      exit when l_rows.count=0;
      
      for i in 1..l_rows.count loop
         l_clob := l_clob || l_rows(i);
      end loop;
   end loop;
   
   update test2 set my_data = l_clob where id=1;
   --insert into test2(id, my_data) values (1, l_clob); --returning my_data into l_clob;
   
END;
/
alter session set events '10046 trace name context off';

------------------------------------------
-- LOB locator
------------------------------------------
insert into test2 values (1,null);
alter session set tracefile_identifier='LJ06';
alter session set events '10046 trace name context forever, level 12';
DECLARE
   l_clob clob;
   l_temp clob;
   l_header varchar2(200) := 'owner;object_name;object_type;last_ddl_time' || chr(10);
BEGIN
   update test2 set my_data = empty_clob() where id=1 returning my_data into l_temp;
   
   dbms_lob.writeappend(l_temp,length(l_header),l_header);
   
   for rec in (select owner || ';' || object_name || ';' || object_type || ';' || to_char(last_ddl_time, 'DD.MM.YYYY HH24:MI:SS') || chr(10) my_data
               from test 
               where rownum<=1000000) loop
      l_clob := l_clob || rec.my_data;
   end loop;
   
   dbms_lob.append(l_temp, l_clob);  
   --insert into test2(id, my_data) values (1, l_clob); --returning my_data into l_clob;
END;
/
alter session set events '10046 trace name context off';


------------------------------------------
-- LOB locator with bulk collect
------------------------------------------
insert into test2 values (1,null);
alter session set tracefile_identifier='BULK03';
alter session set events '10046 trace name context forever, level 12';
DECLARE
   l_clob clob;
   l_temp clob;
   l_header varchar2(200) := 'owner;object_name;object_type;last_ddl_time' || chr(10);
   
   type vc100_nt is table of varchar2(100);
   l_rows vc100_nt;
   
   cursor c_rows is 
      select owner || ';' || object_name || ';' || object_type || ';' || to_char(last_ddl_time, 'DD.MM.YYYY HH24:MI:SS') || chr(10) my_data
      from test where rownum<=1000000;
BEGIN
   update test2 set my_data = empty_clob() where id=1 returning my_data into l_temp;
   
   dbms_lob.writeappend(l_temp,length(l_header),l_header);
   
   open c_rows;
   loop
      fetch c_rows bulk collect into l_rows limit 1000;
      exit when l_rows.count=0;
      
      l_clob := '';
      
      for i in 1..l_rows.count loop
         l_clob := l_clob || l_rows(i);
      end loop;
      dbms_lob.append(l_temp, l_clob);
   end loop;
     
   --insert into test2(id, my_data) values (1, l_clob); --returning my_data into l_clob;
END;
/
alter session set events '10046 trace name context off';

select * from test2;