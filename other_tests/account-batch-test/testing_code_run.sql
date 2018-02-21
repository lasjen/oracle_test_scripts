set timing on serveroutput on 
exec cca_api.run_end_of_day;


select a.* , r.RENT_PERCENTAGE
from lj_accounts a, lj_rent r
where a.accountnr between 1 and 15
  and a.bankid=r.bankid;

with my_acc as (select '00000000003' acc from dual)
select *
from (select * from lj_account_cred_balances where accountnr=(select acc from my_acc)
      union 
      select * from lj_account_deb_balances where accountnr=(select acc from my_acc));