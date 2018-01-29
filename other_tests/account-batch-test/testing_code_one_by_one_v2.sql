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

FUNCTION ms_between (startTS timestamp, endTS timestamp) RETURN number AS
  vMS number default 0;
begin
  select
    (extract(day from endTS - startTS)*86400+
    extract(hour from endTS - startTS)*3600+
    extract(minute from endTS - startTS)*60+
    extract(second from endTS - startTS)) * 1000 into vMS
    from dual;
  return vMS;
end;

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

PROCEDURE replenish(acc_info_io IN OUT NOCOPY accnt_info_ot) IS
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
  --calc_rent(acc_info_io.rent,l_buy, l_card, l_cash, l_rent);
  
  acc_info_io.setCredit('BUY',l_buy);
  acc_info_io.setCredit('CARD',l_card);
  acc_info_io.setCredit('CASH',l_cash);
  acc_info_io.setCredit('RENT',l_rent);
END;

PROCEDURE interestrate(acc_info_io IN OUT NOCOPY accnt_info_ot) IS
  l_rent    number;
BEGIN
  l_rent := acc_info_io.getCredit('RENT');  -- log('GETCREDIT(RENT):' || l_rent);
  
  -- Calculate rent
  calc_rent(acc_info_io.rent, acc_info_io.getCredit('BUY'), 
                              acc_info_io.getCredit('CARD'), 
							  acc_info_io.getCredit('CASH'), l_rent);
  
  acc_info_io.setCredit('RENT',l_rent);
END;

PROCEDURE run_end_of_day IS
  l_accountnr     varchar2(11);
  l_bankid        varchar2(10);
  l_rent          number;
  l_deposit       number;
  l_rc            sys_refcursor;
  l_balance_type  varchar2(10);
  l_balance_value number;
  l_counter       number := 0;
  l_total         number := 0;
  l_start_fetch   timestamp(3);
  l_end_fetch     timestamp(3);
  l_end_calc      timestamp(3);
  l_end_time      timestamp(3);
  l_ms_fetch      number;
  l_ms_calc       number;
  l_ms_storage    number;
  l_ms_total      number;
  
  CURSOR acc_info_cur IS
    SELECT accnt_info_ot(a.accountnr,a.bankid,r.rent_percentage , c.VALUE , 
                         cast( multiset(select balance_info_ot(d.balance_type, D.VALUE) 
                                        from lj_account_cred_balances d 
                                        where a.accountnr=d.accountnr) as balance_info_ct))
    FROM lj_accounts a, lj_rent r, LJ_ACCOUNT_deb_BALANCES c
    WHERE a.bankid=r.bankid
      AND a.accountnr=c.accountnr
    ORDER BY c.accountnr ASC;
      
  l_acc_infos accnt_info_ct;
BEGIN
  OPEN acc_info_cur;
  LOOP
    l_start_fetch := systimestamp;
    FETCH acc_info_cur BULK COLLECT INTO l_acc_infos LIMIT 10000;
    EXIT WHEN l_acc_infos.count=0;
    
    l_end_fetch := systimestamp;
    
    FOR i IN 1..l_acc_infos.count LOOP
      replenish(l_acc_infos(i));
    END LOOP;
    
    l_end_calc := systimestamp;
    
    MERGE /*+ INDEX(cr LJ_ACCOUNT_CRED_BALANCES_PK) */ INTO lj_account_cred_balances cr
         USING (SELECT a.accountnr, c.balance_type, c.balance_value from table(l_acc_infos) a, table(a.credits) c) m
         ON (cr.accountnr=m.accountnr AND cr.balance_type=m.balance_type)
      WHEN MATCHED THEN
        UPDATE SET cr.value=m.balance_value, cr.changed=systimestamp;
      
    MERGE INTO lj_account_deb_balances de
         USING (SELECT a.accountnr, a.deposit from table(l_acc_infos) a) m
         ON (de.accountnr=m.accountnr)
      WHEN MATCHED THEN
        UPDATE SET de.value=m.deposit, de.changed=systimestamp;
    
    l_end_time    := systimestamp;
    
    l_ms_fetch    := ms_between(l_start_fetch, l_end_fetch);
    l_ms_calc     := ms_between(l_end_fetch, l_end_calc);
    l_ms_storage  := ms_between(l_end_calc, l_end_time);
    l_ms_total    := ms_between(l_start_fetch, l_end_time);
    
    INSERT INTO lj_timings (id, start_fetch, end_fetch, end_calc, end_total, ms_fetch, ms_calc, ms_storage, ms_total) 
      VALUES (lj_timings_seq.nextval, l_start_fetch, l_end_fetch, l_end_calc, l_end_time, l_ms_fetch, l_ms_calc, l_ms_storage, l_ms_total);
    COMMIT;
    
    --l_total := l_total + l_acc_infos.count;
    --dbms_application_info.set_client_info('Count: ' || l_total);
         
  END LOOP;  
  close acc_info_cur;
  
  OPEN acc_info_cur;
  LOOP
    l_start_fetch := systimestamp;
    FETCH acc_info_cur BULK COLLECT INTO l_acc_infos LIMIT 10000;
    EXIT WHEN l_acc_infos.count=0;
    
    l_end_fetch := systimestamp;
    
    FOR i IN 1..l_acc_infos.count LOOP
      interestrate(l_acc_infos(i));
    END LOOP;
    
    l_end_calc := systimestamp;
    
    MERGE /*+ INDEX(cr LJ_ACCOUNT_CRED_BALANCES_PK) */ INTO lj_account_cred_balances cr
         USING (SELECT a.accountnr, c.balance_type, c.balance_value from table(l_acc_infos) a, table(a.credits) c) m
         ON (cr.accountnr=m.accountnr AND cr.balance_type=m.balance_type)
      WHEN MATCHED THEN
        UPDATE SET cr.value=m.balance_value, cr.changed=systimestamp;
      
    MERGE /*+ INDEX_RS_ASC(de LJ_ACCOUNT_DEB_BALANCES_PK) */ INTO lj_account_deb_balances de
         USING (SELECT a.accountnr, a.deposit from table(l_acc_infos) a) m
         ON (de.accountnr=m.accountnr)
      WHEN MATCHED THEN
        UPDATE SET de.value=m.deposit, de.changed=systimestamp;
        
    l_end_time := systimestamp;
    l_ms_fetch    := ms_between(l_start_fetch, l_end_fetch);
    l_ms_calc     := ms_between(l_end_fetch, l_end_calc);
    l_ms_storage  := ms_between(l_end_calc, l_end_time);
    l_ms_total    := ms_between(l_start_fetch, l_end_time);
    
    INSERT INTO lj_timings (id, start_fetch, end_fetch, end_calc, end_total, ms_fetch, ms_calc, ms_storage, ms_total) 
      VALUES (lj_timings_seq.nextval, l_start_fetch, l_end_fetch, l_end_calc, l_end_time, l_ms_fetch, l_ms_calc, l_ms_storage, l_ms_total);
    COMMIT;
    
    l_total := l_total + l_acc_infos.count;
    dbms_application_info.set_client_info('Count: ' || l_total);
         
  END LOOP;  
  close acc_info_cur;
END;

END cca_api;
/

show errors;