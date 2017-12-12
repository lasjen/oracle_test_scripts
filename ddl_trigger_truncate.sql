drop table tst purge;
drop table tst2 purge;

create table tst(id number, tekst varchar2(10));
create table tst2(id number, tekst varchar2(10));
insert into tst values (1,'DUMMY');
insert into tst2 values (1,'DUMMY');
commit;


CREATE OR REPLACE TRIGGER prevent_truncates BEFORE TRUNCATE ON SCHEMA
BEGIN
   IF (ora_dict_obj_name) = 'TST' THEN
      raise_application_error(-20001,'TRUNCATE not permitted');
   END IF;
END;
/

truncate table tst2;
truncate table tst;

select * from tst2;
select * from tst;