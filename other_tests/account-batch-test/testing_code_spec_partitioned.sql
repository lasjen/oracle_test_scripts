CREATE OR REPLACE PACKAGE cca_api as
  
  procedure run_end_of_day(date_i IN date);
   
END cca_api;
/

show errors;