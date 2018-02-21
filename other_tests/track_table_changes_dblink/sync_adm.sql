create or replace package sync_adm as
  TYPE big_table_t is table of big_table%rowtype ;
  procedure do_sync;
end sync_adm;
/

create or replace package body sync_adm as
  procedure do_sync is
    bigt big_table_t := big_table_t();
    bigi big_table_t := big_table_t();
    bigs big_table_t := big_table_t();
  begin
    for rec in (select * 
                from (select * from big_table order by object_id) b
                where rownum<100) loop
      if rec.object_type = 'TABLE' then
        bigt.extend;
        bigt(bigt.count).OWNER            := rec.owner;
        bigt(bigt.count).OBJECT_NAME      := rec.object_name;
        bigt(bigt.count).SUBOBJECT_NAME   := rec.subobject_name;
        bigt(bigt.count).OBJECT_ID        := rec.object_id;
        bigt(bigt.count).DATA_OBJECT_ID   := rec.data_object_id;
        bigt(bigt.count).OBJECT_TYPE      := rec.object_type;
        bigt(bigt.count).CREATED          := rec.created;
        bigt(bigt.count).LAST_DDL_TIME    := rec.last_ddl_time;
        bigt(bigt.count).TIMESTAMP        := rec.timestamp;
        bigt(bigt.count).STATUS           := rec.status;
        bigt(bigt.count).TEMPORARY        := rec.temporary;
        bigt(bigt.count).GENERATED        := rec.generated;
        bigt(bigt.count).SECONDARY        := rec.secondary;
        bigt(bigt.count).NAMESPACE        := rec.namespace;
        bigt(bigt.count).EDITION_NAME     := rec.edition_name;
        bigt(bigt.count).SHARING          := rec.sharing;
        bigt(bigt.count).EDITIONABLE      := rec.editionable;
        bigt(bigt.count).ORACLE_MAINTAINED:= rec.oracle_maintained;
        bigt(bigt.count).LEVEL            := rec.level;
      end if;
    end loop;
    
    FORALL i IN 1..bigt.count
      INSERT INTO big_global VALUES (bigt(i).owner, bigt(i).object_name, bigt(i).subobject_name, bigt(i).object_id, bigt(i).data_object_id,
                                       bigt(i).object_type, bigt(i).created, bigt(i).last_ddl_time, bigt(i).timestamp, bigt(i).status,
                                       bigt(i).temporary, bigt(i).generated, bigt(i).secondary, bigt(i).namespace, bigt(i).edition_name,
                                       bigt(i).sharing, bigt(i).editionable, bigt(i).oracle_maintained, bigt(i).level);
      
    INSERT INTO big_table_i@db_et SELECT * FROM big_global;
    commit;
    
  end;
end sync_adm;
/

exec sync_adm.do_sync;

show error
select object_id, count(*) from big_Table group by object_id having count(*)>1;

select * 
from (select * from big_table order by object_id) b
where rownum<100;