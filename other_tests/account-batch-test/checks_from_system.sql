select * from appdata.lj_timings order by id desc;


select round(sum(ms_fetch)/1000,2)   sec_fetch, 
       round(sum(ms_calc)/1000,2)    sec_calc,
       round(sum(ms_storage)/1000,2) sec_storage,
       round(sum(ms_total)/1000,2)   sec_total
from appdata.lj_timings;

select * from v$sql where sql_text like 'MERGE%';           --6rd3gnkad6a9j, bkj5rq69bk310
select * from v$sql where sql_text like 'SELECT ACCNT%';    --24smfs3bf9frd

set pages 9999 lines 200
select * from table(dbms_xplan.display_cursor('24smfs3bf9frd',null, 'TYPICAL')); --6rd3gnkad6a9j, bkj5rq69bk310

