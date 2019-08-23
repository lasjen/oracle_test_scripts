DROP TABLE EMP;
DROP TABLE DEPT;

CREATE TABLE DEPT
       (DEPTNO NUMBER(2) CONSTRAINT PK_DEPT PRIMARY KEY,
	DNAME VARCHAR2(14) ,
	LOC VARCHAR2(13) ) ;
  
DROP TABLE EMP;
CREATE TABLE EMP(
  EMPNO NUMBER(4) CONSTRAINT PK_EMP PRIMARY KEY,
	ENAME VARCHAR2(10),
	JOB VARCHAR2(9),
	MGR NUMBER(4),
	HIREDATE DATE,
	SAL NUMBER(7,2),
	COMM NUMBER(7,2),
	DEPTNO NUMBER(2) CONSTRAINT FK_DEPTNO REFERENCES DEPT);
  
INSERT ALL 
  INTO DEPT VALUES (10,'ACCOUNTING','NEW YORK')
  INTO DEPT VALUES (20,'RESEARCH','DALLAS')
  INTO DEPT VALUES (30,'SALES','CHICAGO')
  INTO DEPT VALUES (40,'OPERATIONS','BOSTON')
SELECT * FROM DUAL;

INSERT ALL
  INTO EMP VALUES (7369,'SMITH','CLERK',7902,to_date('17-12-1980','dd-mm-yyyy'),800,NULL,20)
  INTO EMP VALUES (7499,'ALLEN','SALESMAN',7698,to_date('20-2-1981','dd-mm-yyyy'),1600,300,30)
  INTO EMP VALUES (7521,'WARD','SALESMAN',7698,to_date('22-2-1981','dd-mm-yyyy'),1250,500,30)
  INTO EMP VALUES (7566,'JONES','MANAGER',7839,to_date('2-4-1981','dd-mm-yyyy'),2975,NULL,20)
  INTO EMP VALUES (7654,'MARTIN','SALESMAN',7698,to_date('28-9-1981','dd-mm-yyyy'),1250,1400,30)
  INTO EMP VALUES (7698,'BLAKE','MANAGER',7839,to_date('1-5-1981','dd-mm-yyyy'),2850,NULL,30)
  INTO EMP VALUES (7782,'CLARK','MANAGER',7839,to_date('9-6-1981','dd-mm-yyyy'),2450,NULL,10)
  INTO EMP VALUES (7788,'SCOTT','ANALYST',7566,to_date('13-JUL-87')-85,3000,NULL,20)
  INTO EMP VALUES (7839,'KING','PRESIDENT',NULL,to_date('17-11-1981','dd-mm-yyyy'),5000,NULL,10)
  INTO EMP VALUES (7844,'TURNER','SALESMAN',7698,to_date('8-9-1981','dd-mm-yyyy'),1500,0,30)
  INTO EMP VALUES (7876,'ADAMS','CLERK',7788,to_date('13-JUL-87')-51,1100,NULL,20)
  INTO EMP VALUES (7900,'JAMES','CLERK',7698,to_date('3-12-1981','dd-mm-yyyy'),950,NULL,30)
  INTO EMP VALUES (7902,'FORD','ANALYST',7566,to_date('3-12-1981','dd-mm-yyyy'),3000,NULL,20)
  INTO EMP VALUES (7934,'MILLER','CLERK',7782,to_date('23-1-1982','dd-mm-yyyy'),1300,NULL,10)
SELECT * FROM DUAL;

DROP TABLE BONUS;
CREATE TABLE BONUS
	(
	ENAME VARCHAR2(10)	,
	JOB VARCHAR2(9)  ,
	SAL NUMBER,
	COMM NUMBER
	) ;
DROP TABLE SALGRADE;
CREATE TABLE SALGRADE
      ( GRADE NUMBER,
	LOSAL NUMBER,
	HISAL NUMBER );
INSERT ALL 
  INTO SALGRADE VALUES (1,700,1200)
  INTO SALGRADE VALUES (2,1201,1400)
  INTO SALGRADE VALUES (3,1401,2000)
  INTO SALGRADE VALUES (4,2001,3000)
  INTO SALGRADE VALUES (5,3001,9999)
SELECT * FROM DUAL;
COMMIT;

-- Create Collection Types
DROP TYPE emp_at;
DROP TYPE emp_t;
CREATE OR REPLACE TYPE emp_t AS OBJECT (
  EMPNO NUMBER(4),
	ENAME VARCHAR2(10),
	JOB VARCHAR2(9),
	MGR NUMBER(4),
	HIREDATE DATE,
	SAL NUMBER(7,2),
	COMM NUMBER(7,2),
  DEPTNO NUMBER(2)
)
/
CREATE OR REPLACE TYPE emp_at AS TABLE OF emp_t
/

CREATE OR REPLACE PROCEDURE list_employees(deptno_i IN NUMBER, dname_o OUT varchar2, emp_list_o OUT emp_at) AS
BEGIN
  select d.DNAME , (SELECT cast(collect(emp_t(e.empno, e.ename, e.job, e.mgr, e.hiredate, e.sal, e.comm, e.deptno)) as emp_at)
                    FROM emp e
                    WHERE e.deptno=d.deptno) INTO dname_o, emp_list_o
  from dept d 
  where d.deptno=deptno_i;
END list_employees;
/

CREATE OR REPLACE PROCEDURE list_employees_json(deptno_i IN NUMBER, emp_details_o OUT varchar2) AS
BEGIN
       with manager as
    ( select '{ '
           ||' "name":"'||ename||'"'
           ||',"salary":'||sal
           ||',"hiredate":"'||to_char(hiredate, 'DD-MM-YYYY')||'"'
           ||'} ' json
      , emp.*
      from   emp 
    )
    , employee as
    ( select '{ '
           ||' "empno":"'||empno||'"'
           ||' "name":"'||ename||'"'
           ||',"job":"'||job||'"'
           ||',"salary":'||sal
           ||',"manager":'||case when mgr is null then '""' else (select json from manager mgr where mgr.empno = emp.mgr) end       ||',"hiredate":"'||to_char(hiredate, 'DD-MM-YYYY')||'"'
           ||'} ' json
      , emp.*
      from   emp
    )
    , department as
    ( select '{ '
           ||' "name":"'||dname||'"'
           ||',"deptno":"'||deptno||'"'
           ||',"location":"'||loc||'"'
           ||',"employees":'||(  select '['||listagg( json, ',')
                                                      within group (order by 1)
                                      ||']' as data
                                 from employee emp
                                 where emp.deptno = dept.deptno
                              )
           ||'} ' json
      from   dept
      where deptno=deptno_i
    )
    select '{"company" : ['
           ||( select listagg( json, ',')
                      within group (order by 1)
               from   department
              )
           ||']}' into emp_details_o
    from   dual;

END list_employees_json;
/

select emp_t(empno, ename, job, mgr, hiredate, sal, comm, deptno) as emp_at 
from emp;

select d.DNAME , (SELECT cast(collect(emp_t(e.empno, e.ename, e.job, e.mgr, e.hiredate, e.sal, e.comm, e.deptno)) as emp_at)
                  FROM emp e
                  WHERE e.deptno=d.deptno) emp_list
  from dept d 
  where d.deptno=10;

select cast(collect(emp_t(empno, ename, job, mgr, hiredate, sal, comm, deptno)) as emp_at) as emp_list 
from emp;

select d.*, (select cast(collect(emp_t(empno, ename, job, mgr, hiredate, sal, comm, deptno)) as emp_at)  from emp e where e.deptno=d.deptno) emps
from dept d;

   with manager as
    ( select '{ '
           ||' "name":"'||ename||'"'
           ||',"salary":'||sal
           ||',"hiredate":"'||to_char(hiredate, 'DD-MM-YYYY')||'"'
           ||'} ' json
      , emp.*
      from   emp 
    )
    , employee as
    ( select '{ '
           ||' "name":"'||ename||'"'
           ||',"job":"'||job||'"'
           ||',"salary":'||sal
           ||',"manager":'||case when mgr is null then '""' else (select json from manager mgr where mgr.empno = emp.mgr) end       ||',"hiredate":"'||to_char(hiredate, 'DD-MM-YYYY')||'"'
           ||'} ' json
      , emp.*
      from   emp
    )
    , department as
    ( select '{ '
           ||' "name":"'||dname||'"'
           ||',"identifier":"'||deptno||'"'
           ||',"location":"'||loc||'"'
           ||',"employees":'||(  select '['||listagg( json, ',')
                                                      within group (order by 1)
                                      ||']' as data
                                 from employee emp
                                 where emp.deptno = dept.deptno
                              )
           ||'} ' json
      from   dept
      where deptno=10
    )
    select '{"company" : ['
           ||( select listagg( json, ',')
                      within group (order by 1)
               from   department
              )
           ||']}'
    from   dual;