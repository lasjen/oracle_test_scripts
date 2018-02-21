create user et1 identified by et1;
grant dba to et1;
drop table big_table_i purge;
drop table big_table_t purge;
drop table big_table_s purge;
create table big_table_i as select * from ljdata.big_table where 1=2;
create table big_table_t as select * from ljdata.big_table where 1=2;
create table big_table_s as select * from ljdata.big_table where 1=2;