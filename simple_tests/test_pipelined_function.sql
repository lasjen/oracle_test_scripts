CREATE VIEW solr_srch_ass_1_9 AS SELECT object_id, owner, object_name FROM big_table;
/

CREATE OR REPLACE TYPE t_solr_bt_row AS OBJECT (
  object_id     NUMBER,
  owner         VARCHAR2(128),
  object_name   VARCHAR2(128))
/

CREATE TYPE t_solr_bt_tab IS TABLE OF t_solr_bt_row
/

CREATE OR REPLACE PACKAGE solr_util AS 
  CURSOR big_table_cur IS 
    SELECT t_solr_bt_row(object_id, owner, object_name) FROM big_table;
    
  FUNCTION getData return t_solr_bt_tab pipelined; 
END solr_util;
/

CREATE OR REPLACE PACKAGE BODY solr_util AS 

  FUNCTION getData return t_solr_bt_tab pipelined IS
    l_row t_solr_bt_row;
  BEGIN
    OPEN big_table_cur;
    
    LOOP
      FETCH big_table_cur INTO l_row;
      EXIT WHEN big_table_cur%NOTFOUND;
      PIPE ROW (l_row);
    END LOOP;
    CLOSE big_table_cur;
  END getData;
END solr_util;
/

alter session set tracefile_identifier='TEST2';
alter session set events '10046 trace name context forever, level 12';
SELECT * FROM table(solr_util.getData);
alter session set events '10046 trace name context off';
