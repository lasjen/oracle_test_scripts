set serveroutput on size 1000000

DECLARE
  -------------------------------------
  -- Settings
  -------------------------------------
  s_insert_data       boolean     := TRUE;
  s_partitions        boolean     := FALSE;
  s_account_rows      number      := 1000000;
  
  -------------------------------------
  -- Exceptions
  -------------------------------------
  table_not_exist     exception; 
  PRAGMA EXCEPTION_INIT(table_not_exist, -942); 
  object_not_exist    exception;
  PRAGMA EXCEPTION_INIT(object_not_exist, -4043); 
  
  -------------------------------------
  -- Variables
  -------------------------------------
  l_sql 			        varchar2(4000);
  
  -------------------------------------
  -- prodedures and functions
  -------------------------------------
  PROCEDURE log(txt_i varchar2) AS
  BEGIN
	 dbms_output.put_line(txt_i);
  END;
  
  PROCEDURE run_sql(sql_i varchar2) AS
  BEGIN
    l_sql := sql_i;
    execute immediate sql_i;
	  log('SUCCESS: ' || sql_i);
  END;
  
  PROCEDURE run_drop(sql_i varchar2) AS
  BEGIN
     l_sql := sql_i;
     execute immediate sql_i;
	   log('SUCCESS: ' || sql_i);
  EXCEPTION 
    WHEN table_not_exist THEN
      null;
    WHEN object_not_exist THEN
      null;
    WHEN OTHERS THEN
      raise;
  END;

  -------------------------------------
  -- MAIN - START
  -------------------------------------  
BEGIN
  ------------------------------------------------------
  --- DELETE OBJECTS
  ------------------------------------------------------
  run_drop('drop table lj_account_deb_balances purge');
  run_drop('drop table lj_account_cred_balances purge');
  run_drop('drop table lj_accounts purge');
  run_drop('drop table lj_balance_type purge');
  run_drop('drop table lj_rent purge');
  run_drop('drop table lj_timings purge');
  run_drop('drop sequence lj_timings_seq');
  run_drop('drop type accnt_info_ct');
  run_drop('drop type accnt_info_ot');
  run_drop('drop type balance_info_ct');
  run_drop('drop type balance_info_ot');
  
  -------------------------------------------
  -- TABLE: LJ_BALANCE_TYPES
  -------------------------------------------
  run_sql('create table lj_rent(
   bankid varchar2(10) primary key,
   rent_percentage number)');
  
  IF s_insert_data THEN
    run_sql('insert into lj_rent values (''4200'',6)');
    run_sql('insert into lj_rent values (''4300'',9)');
    run_sql('insert into lj_rent values (''4400'',4)');
  END IF;
  -------------------------------------------
  -- TABLE: LJ_BALANCE_TYPES
  -------------------------------------------
  run_sql('create table lj_balance_type(
   balance_type varchar2(10) primary key)');
  
  IF s_insert_data THEN
    run_sql('insert into lj_balance_type values (''DEPOSIT'')');
    run_sql('insert into lj_balance_type values (''CASH'')');
    run_sql('insert into lj_balance_type values (''BUY'')');
    run_sql('insert into lj_balance_type values (''CARD'')');
    run_sql('insert into lj_balance_type values (''RENT'')');
  END IF;
  
  -------------------------------------------
  -- TABLE: LJ_ACCOUNTS
  -------------------------------------------
  run_sql('create table lj_accounts as
            select rownum accountnr,
               OWNER bankid, OBJECT_NAME last_name, SUBOBJECT_NAME first_name,
               OBJECT_ID, DATA_OBJECT_ID,
               OBJECT_TYPE, CREATED, LAST_DDL_TIME,
               TIMESTAMP, STATUS, TEMPORARY,
               GENERATED, SECONDARY
            from all_objects a where 1=0');
  run_sql('alter table lj_accounts nologging');
  run_sql('alter table lj_accounts modify accountnr varchar2(11)');
  run_sql('alter table lj_accounts modify bankid varchar2(10)');
  
  -- Inserting data
  IF s_insert_data THEN
    declare
      l_cnt number;
    begin
      run_sql('insert /*+ append */
        into lj_accounts
        select lpad(rownum,11,''0''),
               case mod(rownum,3) when 0 then ''4200''
                                  when 1 then ''4300''
                                  else ''4400'' end, OBJECT_NAME, SUBOBJECT_NAME,
               OBJECT_ID, DATA_OBJECT_ID,
               OBJECT_TYPE, CREATED, LAST_DDL_TIME,
               TIMESTAMP, STATUS, TEMPORARY,
               GENERATED, SECONDARY
        from all_objects a
        where rownum <=' || s_account_rows);

      l_cnt := sql%rowcount;
      commit;
      while (l_cnt < s_account_rows) loop
          run_sql('insert /*+ APPEND */ into lj_accounts
            select lpad(rownum+' || l_cnt || ',11,''0''),
                   case mod(rownum,3) when 0 then ''4200''
                                      when 1 then ''4300''
                                      else ''4400'' end, 
                   OBJECT_NAME, SUBOBJECT_NAME,
                   OBJECT_ID, DATA_OBJECT_ID,
                   OBJECT_TYPE, CREATED, LAST_DDL_TIME,
                   TIMESTAMP, STATUS, TEMPORARY,
                   GENERATED, SECONDARY
            from all_objects
            where rownum <=' || (s_account_rows - l_cnt));
          l_cnt := l_cnt + sql%rowcount;
          commit;
      end loop;
      log('SUCCESS: inserted '|| l_cnt || ' into the LJ_ACCOUNTS table');
    end;
  END IF;
  
  run_sql('alter table lj_accounts add constraint lj_accounts_pk primary key(accountnr)');
  run_sql('alter table lj_accounts logging');
  -------------------------------------------
  -- TABLE: LJ_ACCOUNT_CRED_BALANCES
  -------------------------------------------
  l_sql := 'create table lj_account_cred_balances (
  accountnr    varchar2(11),
  balance_type varchar2(10),
  value        number,
  changed      timestamp)';
  
  IF s_partitions THEN
    l_sql := l_sql || '
partition by range(changed) interval (numtodsinterval(1,''day''))
store in (users)(
   partition before_2017 values less than (to_date(''01-01-2016'',''dd-mm-yyyy''))
)';
  END IF;
  
  run_sql(l_sql);
  run_sql('alter table lj_account_cred_balances nologging');
  
  IF s_insert_data THEN
    run_sql('INSERT /*+ APPEND */ INTO lj_account_cred_balances(accountnr, balance_type, value, changed)
         SELECT a.accountnr, b.balance_type, b.value, systimestamp-1
         FROM lj_accounts a , (select ''CASH'' as balance_type, 2500 as value from dual
                               union all 
                               select ''BUY'', 2000 from dual
                               union all
                               select ''CARD'', 1000 from dual
                               union all
                               select ''RENT'', 0 from dual) b');
    log('INSERT: Inserted '|| sql%rowcount || ' into the LJ_ACCOUNT_CRED_BALANCES table');
  END IF;
  
  run_sql('create unique index lj_account_cred_balances_pk on lj_account_cred_balances(accountnr, balance_type)');
  run_sql('alter table lj_account_cred_balances add constraint lj_account_cred_balances_pk primary key (accountnr, balance_type) using index lj_account_cred_balances_pk');  
  run_sql('alter table lj_account_cred_balances logging');
  
  -------------------------------------------
  -- TABLE: LJ_ACCOUNT_CRED_BALANCES
  -------------------------------------------
  l_sql := 'create table lj_account_deb_balances (
  accountnr    varchar2(11),
  balance_type varchar2(10),
  value        number,
  changed      timestamp)';
  
  IF s_partitions THEN
    l_sql := l_sql || '
partition by range(changed) interval (numtodsinterval(1,''day''))
store in (users)(
   partition before_2017 values less than (to_date(''01-01-2016'',''dd-mm-yyyy''))
)';
  END IF;
  
  run_sql(l_sql);
  run_sql('alter table lj_account_deb_balances nologging');

  IF s_insert_data THEN
    run_sql('INSERT /*+ APPEND */ INTO lj_account_deb_balances(accountnr, balance_type, value, changed)
        SELECT a.accountnr, b.balance_type, b.value, systimestamp - 1
        FROM lj_accounts a , (select ''DEPOSIT'' balance_type, 3500 value from dual) b');
    log('INSERT: Inserted '|| sql%rowcount || ' into the LJ_ACCOUNT_DEB_BALENCES table');
  END IF;
  
  run_sql('create unique index lj_account_deb_balances_pk on lj_account_deb_balances(accountnr, balance_type)');
  run_sql('alter table lj_account_deb_balances add constraint lj_account_deb_balances_pk primary key (accountnr, balance_type) using index lj_account_deb_balances_pk');
  run_sql('alter table lj_account_deb_balances logging');
  
  -------------------------------------------
  -- TABLE: LJ_TIMINGS and LJ_TIMINGS_SEQ
  -------------------------------------------
  run_sql('create table lj_timings(
   id          number primary key,
   start_fetch timestamp(3),
   end_fetch   timestamp(3),
   end_calc    timestamp(3),
   end_total   timestamp(3),
   ms_fetch    number,
   ms_calc     number,
   ms_storage  number,
   ms_total    number)');
  
  run_sql('create sequence lj_timings_seq start with 1 increment by 1 cache 100');
  
  ---------------------------------------
  -- Foreign keys
  ---------------------------------------
  run_sql('alter table lj_account_deb_balances add constraint lj_acc_deb_acc_fk foreign key (accountnr) references lj_accounts(accountnr)');
  run_sql('alter table lj_account_deb_balances add constraint lj_acc_deb_bal_type_fk foreign key (balance_type) references lj_balance_type(balance_type)');
  run_sql('alter table lj_account_cred_balances add constraint lj_acc_cred_acc_fk foreign key (accountnr) references lj_accounts(accountnr)');
  run_sql('alter table lj_account_cred_balances add constraint lj_acc_cred_bal_type_fk foreign key (balance_type) references lj_balance_type(balance_type)');
  run_sql('alter table lj_accounts add constraint lj_acc_rent_fk foreign key (bankid) references lj_rent(bankid)');

  ---------------------------------------
  -- Statistics
  ---------------------------------------
  dbms_stats.gather_table_stats( ownname => user, tabname => 'lj_rent' , cascade => TRUE );
  dbms_stats.gather_table_stats( ownname => user, tabname => 'lj_accounts' , cascade => TRUE );
  dbms_stats.gather_table_stats( ownname => user, tabname => 'lj_balance_type' , cascade => TRUE );
  dbms_stats.gather_table_stats( ownname => user, tabname => 'lj_account_deb_balances' , cascade => TRUE );
  dbms_stats.gather_table_stats( ownname => user, tabname => 'lj_account_cred_balances' , cascade => TRUE );
  
  ---------------------------------------
  -- Object Types
  ---------------------------------------
  run_sql('CREATE OR REPLACE TYPE balance_info_ot AS OBJECT (
    balance_type      varchar2(10),
    balance_value     number)');
    
  run_sql('CREATE OR REPLACE TYPE balance_info_ct AS TABLE OF balance_info_ot');
  
  run_sql('CREATE OR REPLACE TYPE accnt_info_ot AS OBJECT (
    accountnr         varchar2(11),
    bankid            varchar2(10),
    rent              number,
    deposit           number,
    credits           balance_info_ct,
    MEMBER FUNCTION getCredit(type_i IN varchar2) RETURN NUMBER,
    MEMBER PROCEDURE setCredit(type_i IN varchar2, value_i IN number))
    ');
    
  run_sql('CREATE OR REPLACE TYPE BODY accnt_info_ot AS
    MEMBER FUNCTION getCredit(type_i IN varchar2) RETURN NUMBER AS
    BEGIN
      FOR i IN 1..credits.count LOOP
        IF credits(i).balance_type=type_i THEN
          RETURN credits(i).balance_value;
        END IF;
      END LOOP;
    END getCredit;
    MEMBER PROCEDURE setCredit(type_i IN varchar2, value_i IN number) AS
    BEGIN
      FOR i IN 1..credits.count LOOP
        IF credits(i).balance_type=type_i THEN
          credits(i).balance_value := value_i;
        END IF;
      END LOOP;
    END setCredit;
  END;
  ');
  
  run_sql('CREATE OR REPLACE TYPE accnt_info_ct AS TABLE OF accnt_info_ot');
  
  -------------------------------------
  -- MAIN - Exception Handling
  -------------------------------------   
EXCEPTION
	WHEN OTHERS THEN
		log('ERROR: ' || l_sql);
    log('SQLERR:' || sqlerrm);
		RAISE;

  -------------------------------------
  -- MAIN - END
  ------------------------------------- 
END;
/

select count(*) from lj_rent;
select count(*) from lj_balance_type;
select count(*) from lj_accounts;
select count(*) from lj_account_deb_balances;
select count(*) from lj_account_cred_balances;
select count(*) from lj_timings;