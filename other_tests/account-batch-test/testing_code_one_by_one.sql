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

PROCEDURE calculate(acc_info_io IN OUT NOCOPY accnt_info_ot) IS
-- --------------------------------------------------------
-- 1. Spred DEPOSIT out on accounts on following order:
--    a) BUY
--    b) CARD
--    c) CASH
--    d) RENT
-- 2. Add rent
-- 3. Create invoice
-- --------------------------------------------------------
  l_buy     number;
  l_card    number;
  l_cash    number;
  l_rent    number;
BEGIN
  l_buy  := acc_info_io.getCredit('BUY');   -- log('GETCREDIT(BUY):' || l_buy);
  l_card := acc_info_io.getCredit('CARD');  -- log('GETCREDIT(CARD):' || l_card);
  l_cash := acc_info_io.getCredit('CASH');  -- log('GETCREDIT(CASH):' || l_cash);
  l_rent := acc_info_io.getCredit('RENT');  -- log('GETCREDIT(RENT):' || l_rent);
  
  deposit(acc_info_io.deposit,l_buy);
  deposit(acc_info_io.deposit,l_card);
  deposit(acc_info_io.deposit,l_cash);
  deposit(acc_info_io.deposit,l_rent);
  
  calc_rent(acc_info_io.rent,l_buy, l_card, l_cash, l_rent);
  
  acc_info_io.setCredit('BUY',l_buy);
  acc_info_io.setCredit('CARD',l_card);
  acc_info_io.setCredit('CASH',l_cash);
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
  
  l_acc_infos     accnt_info_ct := accnt_info_ct();
  l_acc_info      accnt_info_ot;
  l_deb_infos     balance_info_ct; 
  l_deb_info      balance_info_ot;
BEGIN
  OPEN acc_info_cur;
  LOOP
    FETCH acc_info_cur INTO l_accountnr, l_bankid, l_rent, l_deposit, l_rc ;
    
    IF (l_counter=10000 or acc_info_cur%NOTFOUND) THEN
      --FORALL i IN 1..l_acc_infos.COUNT
      --  UPDATE;
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
      commit;   
      l_counter := 0;
      l_acc_infos := accnt_info_ct();
    END IF;
    EXIT WHEN acc_info_cur%NOTFOUND;
    
    l_total := l_total +1;
    dbms_application_info.set_client_info('Count: ' || l_total);
    
    l_acc_info := accnt_info_ot(l_accountnr, l_bankid, l_rent, l_deposit, null);
    
    l_deb_infos := balance_info_ct();
    LOOP
      FETCH l_rc INTO l_balance_type, l_balance_value;
      EXIT WHEN l_rc%NOTFOUND;
      
      l_deb_infos.extend;
      l_deb_infos(l_deb_infos.count) := balance_info_ot(l_balance_type, l_balance_value);
    END LOOP;
    CLOSE l_rc;
    l_acc_info.credits := l_deb_infos;
    
    --print(l_acc_info);
    calculate(l_acc_info);
    --print(l_acc_info);
    
    l_acc_infos.extend;
    l_acc_infos(l_acc_infos.count) := l_acc_info;

    --dbms_output.put_line('Looping');
  
    l_counter := l_counter + 1;  
  END LOOP;  
  close acc_info_cur;
END;

END cca_api;
/

show errors;