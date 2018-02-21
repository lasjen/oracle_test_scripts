create or replace TRIGGER BIG_TABLE_CHANGES 
  AFTER DELETE OR INSERT OR UPDATE OF CREATED,DATA_OBJECT_ID,IS_EDITIONABLE,EDITION_NAME,GENERATED,LAST_DDL_TIME,LEVEL_COL,NAMESPACE,OBJECT_ID,OBJECT_NAME,OBJECT_TYPE,ORACLE_MAINTAINED,OWNER,SECONDARY,SHARING,STATUS,SUBOBJECT_NAME,TEMPORARY,TIMESTAMP ON BIG_TABLE 
BEGIN
  IF updating or inserting THEN
    INSERT INTO sq_big_table(owner, object_name, subobject_name,object_id, data_object_id, object_type,created,last_ddl_time,timestamp, status,
                             temporary, generated, secondary, namespace, edition_name, sharing, is_editionable, oracle_maintained, level_col)
    VALUES(
      new.OWNER,
      new.OBJECT_NAME,
      new.SUBOBJECT_NAME,
      new.OBJECT_ID,
      new.DATA_OBJECT_ID,
      new.OBJECT_TYPE,
      new.CREATED,
      new.LAST_DDL_TIME,
      new.TIMESTAMP,
      new.STATUS,
      new.TEMPORARY,
      new.GENERATED,
      new.SECONDARY,
      new.NAMESPACE,
      new.EDITION_NAME,
      new.SHARING,
      new.IS_EDITIONABLE,
      new.ORACLE_MAINTAINED,
      new.LEVEL_COL);
  ELSE
    INSERT INTO sq_big_table(owner, object_name, subobject_name,object_id, data_object_id, object_type,created,last_ddl_time,timestamp, status,
                             temporary, generated, secondary, namespace, edition_name, sharing, is_editionable, oracle_maintained, level_col)
    values(
      old.OWNER,
      old.OBJECT_NAME,
      old.SUBOBJECT_NAME,
      old.OBJECT_ID,
      old.DATA_OBJECT_ID,
      old.OBJECT_TYPE,
      old.CREATED,
      old.LAST_DDL_TIME,
      old.TIMESTAMP,
      old.STATUS,
      old.TEMPORARY,
      old.GENERATED,
      old.SECONDARY,
      old.NAMESPACE,
      old.EDITION_NAME,
      old.SHARING,
      old.IS_EDITIONABLE,
      old.ORACLE_MAINTAINED,
      old.LEVEL_COL);
  END IF;
END;
/
show errors;