-- --------------------------------------------------------
-- File: testing_all_at_once.sql
--
-- Description:
-- The following should be completed:
-- 1. Spred DEPOSIT out on accounts on following order:
--    a) BUY
--    b) CARD
--    c) CASH
--    d) RENT
-- 2. Add rent
-- 3. Create invoice
-- --------------------------------------------------------
CREATE OR REPLACE PACKAGE cca_api as
  cursor acc_info_cur as 
    select a.accountnr,a.bankid,r.rent_percentage , c.VALUE deposit_amount, cursor(select * from lj_account_deb_balances d where a.accountnr=d.accountnr) deb_bal
    from lj_accounts a, lj_rent r, LJ_ACCOUNT_CRED_BALANCES c
    where a.bankid=r.bankid
     and a.accountnr=c.accountnr;
     
  type acc_info_t is table of acc_info_cur%rowtype;
  
  procedure run_end_of_day;
   
END cca_api;
/
CREATE OR REPLACE PACKAGE BODY cca_api as
   procedure run_end_of_day is
   begin
      null;
   end;
END cca_api;
/

