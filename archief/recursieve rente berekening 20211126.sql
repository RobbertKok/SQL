drop table s_dm_cdo.rente_test_sql ;

create table s_dm_cdo.rente_test_sql (n integer,datum date, transactie_bg decimal(18,2), int_rate decimal(18,2));

insert into s_dm_cdo.rente_test_sql values (1,date'2021-01-01',  1000, .03);

insert into s_dm_cdo.rente_test_sql values (2,date'2021-02-01',  NULL, .03);

insert into s_dm_cdo.rente_test_sql values (3,date'2021-03-01', NULL, .02);

insert into s_dm_cdo.rente_test_sql values (4,date'2021-04-01', 200, .02); 

/* met recursive functie en Union vorige record met huidige record vergelijken */

with recursive recur (n, start_bal, transactie_bg, interest, end_bal) as

(

select 1, cast(0 as decimal(18,2)) as start_bal, transactie_bg, cast(transactie_bg * int_rate as decimal(18,2)) as interest, transactie_bg + interest as end_bal 

from s_dm_cdo.rente_test_sql 

where n=1

union all

select b.n, a.end_bal as start_bal_NEW, 

b.transactie_bg, 

             (start_bal_NEW + coalesce(b.transactie_bg,0)) * b.int_rate as interest_NEW, 

             start_bal_NEW + coalesce(b.transactie_bg,0)+ interest_NEW as end_bal_NEW 

    from recur as a, s_dm_cdo.rente_test_sql  as b

where  b.n = a.n+1

) select * from recur

 
-- Recursieve view gemaakt

create recursive view s_dm_cdo.rente_test_sql_rv  (n, start_bal, transactie_bg, interest, end_bal) as

(
select 1, cast(0 as decimal(18,2)) as start_bal, transactie_bg, cast(transactie_bg * int_rate as decimal(18,2)) as interest, transactie_bg + interest as end_bal 

from s_dm_cdo.rente_test_sql 

where n=1

union all

select b.n, a.end_bal as start_bal_NEW, 

b.transactie_bg, 

             (start_bal_NEW + coalesce(b.transactie_bg,0)) * b.int_rate as interest_NEW, 

             start_bal_NEW + coalesce(b.transactie_bg,0)+ interest_NEW as end_bal_NEW 

    from rente_test_sql_rv as a, s_dm_cdo.rente_test_sql  as b

where  b.n = a.n+1

);


 
 