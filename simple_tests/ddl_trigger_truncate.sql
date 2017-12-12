DROP TABLE tst PURGE;
DROP TABLE tst2 PURGE;

CREATE TABLE tst(id NUMBER, tekst VARCHAR2(10));
CREATE TABLE tst2(id NUMBER, tekst VARCHAR2(10));
INSERT INTO tst VALUES (1,'DUMMY');
INSERT INTO tst2 VALUES (1,'DUMMY');
COMMIT;


CREATE OR REPLACE TRIGGER prevent_truncates BEFORE TRUNCATE ON SCHEMA
BEGIN
   IF (ora_dict_obj_name) = 'TST' THEN
      raise_application_error(-20001,'TRUNCATE not permitted');
   END IF;
END;
/

TRUNCATE TABLE tst2;
TRUNCATE TABLE tst;

SELECT * FROM tst2;
SELECT * FROM tst;