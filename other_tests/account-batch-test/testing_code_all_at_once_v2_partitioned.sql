CREATE OR REPLACE PACKAGE BODY cca_api as

PROCEDURE log(txt_i IN varchar2) IS
BEGIN
  dbms_output.put_line(txt_i);
END;

PROCEDURE print(info_i IN accnt_info_ot) IS
  l_str varchar2(1000);
BEGIN
  log('------------------------------------------------------------');
  log('Accountnr: ' || info_i.accountnr);
  log('Bankid:    ' || info_i.bankid);
  log('Deposit:   ' || info_i.deposit);
  l_str := 'Credits:   ';
  FOR i in 1..info_i.credits.count LOOP
    l_str := l_str || info_i.credits(i).balance_type || ': ' || info_i.credits(i).balance_value || ', ';
  END LOOP;
  log(trim(TRAILING ',' FROM l_str));
END;

PROCEDURE deposit(dep_io IN OUT NOCOPY number, cred_io IN OUT NOCOPY number) IS
BEGIN
  IF dep_io > 0 THEN
    IF dep_io >= cred_io THEN
      dep_io:= dep_io - cred_io;
      cred_io := 0;
    ELSE 
      cred_io := cred_io - dep_io;
      dep_io := 0;  
    END IF;
  END IF;
END;

FUNCTION calc_rent_per_day(rent_percent_i IN number, value_i IN number) RETURN number IS
  l_days number := 365;
BEGIN
  return round((rent_percent_i / (l_days* 100)) * value_i,5);
END;

PROCEDURE calc_rent(rent_percent_i IN number, buy_i IN number, card_i IN number, 
                    cash_i IN number, rent_io IN OUT NOCOPY number) IS
BEGIN
  rent_io := rent_io + case when buy_i  > 0 then calc_rent_per_day(rent_percent_i, buy_i)  else 0 end + 
                       case when card_i > 0 then calc_rent_per_day(rent_percent_i, card_i) else 0 end +
                       case when cash_i > 0 then calc_rent_per_day(rent_percent_i, cash_i) else 0 end;
END;

PROCEDURE create_temp_tables(data_i in date) AS
  l_str_date varchar2(8);
  l_tblname varchar2(30);
  l_cnt number;
BEGIN
  l_str_date := to_char(date_i, 'YYYYMMYY');
  
  l_tblname  := 'DEPOSIT_'||l_str_date;
  select count(*) into l_cnt from user_tables where table_name=l_tblname;
  IF l_cnt=0 THEN
    execute immediate 'create table ' || l_tblname || ' as select * from lj_account_dep_balance where 1=2';
  ELSE
    execute immediate 'truncate table ' || l_tblname;
  END IF;
  
  l_tblname   := 'CREDIT_' || l_str_date;
  select count(*) into l_cnt from user_tables where table_name=l_tblname; 
  IF l_cnt=0 THEN
    execute immediate 'create table ' || l_tblname || ' as select * from lj_account_cred_balance where 1=2';
  ELSE
    execute immediate 'truncate table ' || l_tblname;
  END IF;
END;

PROCEDURE calculate(acc_info_io IN OUT NOCOPY accnt_info_ot) IS
  l_buy     number;
  l_card    number;
  l_cash    number;
  l_rent    number;
BEGIN
  l_buy  := acc_info_io.getCredit('BUY');   -- log('GETCREDIT(BUY):' || l_buy);
  l_card := acc_info_io.getCredit('CARD');  -- log('GETCREDIT(CARD):' || l_card);
  l_cash := acc_info_io.getCredit('CASH');  -- log('GETCREDIT(CASH):' || l_cash);
  l_rent := acc_info_io.getCredit('RENT');  -- log('GETCREDIT(RENT):' || l_rent);
  
  -- Replenish
  deposit(acc_info_io.deposit,l_buy);
  deposit(acc_info_io.deposit,l_card);
  deposit(acc_info_io.deposit,l_cash);
  deposit(acc_info_io.deposit,l_rent);

  -- Calculate rent
  calc_rent(acc_info_io.rent,l_buy, l_card, l_cash, l_rent);
  
  acc_info_io.setCredit('BUY',l_buy);
  acc_info_io.setCredit('CARD',l_card);
  acc_info_io.setCredit('CASH',l_cash);
  acc_info_io.setCredit('RENT',l_rent);
END;

PROCEDURE run_end_of_day(date_i IN date) IS
  l_accountnr     varchar2(11);
  l_bankid        varchar2(10);
  l_rent          number;
  l_deposit       number;
  l_rc            sys_refcursor;
  l_balance_type  varchar2(10);
  l_balance_value number;
  l_counter       number := 0;
  l_total         number := 0;
  
  CURSOR acc_info_cur(date_i date) IS
    SELECT accnt_info_ot(a.accountnr,a.bankid,r.rent_percentage , d.VALUE , 
                         cast( multiset(select balance_info_ot(c.balance_type, c.VALUE) 
                                        from lj_account_cred_balances c 
                                        where a.accountnr=c.accountnr
                                          and trunc(c.changed)=trunc(date_i)) as balance_info_ct
                                        )
                              )
    FROM lj_accounts a, lj_rent r, LJ_ACCOUNT_deb_BALANCES d
    WHERE a.bankid=r.bankid
      AND a.accountnr=d.accountnr
      AND trunc(d.changed)=trunc(date_i);
      
  l_acc_infos accnt_info_ct;
BEGIN
  create_temp_tables(date_i);
  
  OPEN acc_info_cur(date_i);
  LOOP
    FETCH acc_info_cur BULK COLLECT INTO l_acc_infos LIMIT 10000;
    EXIT WHEN l_acc_infos.count=0;
    
    FOR i IN 1..l_acc_infos.count LOOP
      calculate(l_acc_infos(i));
    END LOOP;
    
    INSERT INTO lj_account_cred_balances /*+ APPEND */ partition for (date_i) 
    SELECT a.accountnr, c.balance_type, c.balance_value, systimestamp 
    FROM table(l_acc_infos) a, table(a.credits) c;
      
    INSERT INTO lj_account_deb_balances /*+ APPEND */ partition for (date_i) 
    SELECT a.accountnr,'DEPOSIT', a.deposit, systimestamp
    FROM table(l_acc_infos) a;
    
    COMMIT;   

    l_total := l_total + l_acc_infos.count;
    dbms_application_info.set_client_info('Count: ' || l_total); 
    
  END LOOP;  
  CLOSE acc_info_cur;
END;

END cca_api;
/

show errors;