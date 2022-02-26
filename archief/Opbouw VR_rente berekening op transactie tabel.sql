with recursive recur (row_nr, contract_oid, start_balance, betaalde_rente_bg, end_balance) as
(
select cast(1 as integer)
,contract_oid
, cast(-0 as 
 decimal(18,2)) as start_balance
, cast(betaalde_rente_bg as  decimal(18,2))
, betaalde_rente_bg + start_balance as end_balance 

from s_dm_cdo.vr_rekening_transactie 
where row_nr=1
and contract_oid = 9917997

union all

select b.row_nr
,b.contract_oid
,a.end_balance as start_balance_NEW
,b.betaalde_rente_bg
,(start_balance_NEW + coalesce(b.betaalde_rente_bg,0)) as end_balance_NEW 
from recur as a
,s_dm_cdo.vr_rekening_transactie  as b
where  b.row_nr = a.row_nr+1
and b.contract_oid = a.contract_oid
and b.contract_oid = 9917997
) select * from recur
;


replace recursive view s_dm_cdo.rente_berekening (row_nr, contract_oid, rek_nr,rekeningsoort_nr,peil_dt,trx_journaal_dt, saldo_voor_rte_boeking_bg, start_balance, betaalde_rente_bg, end_balance) as
(
select cast(1 as integer)
,contract_oid , rek_nr,rekeningsoort_nr,peil_dt,trx_journaal_dt,saldo_voor_rte_boeking_bg
, cast(saldo_voor_rte_boeking_bg as  decimal(18,2)) as start_balance
, cast(betaalde_rente_bg as  decimal(18,2))
, betaalde_rente_bg + start_balance as end_balance 

from s_dm_cdo.vr_rekening_transactie 
where row_nr=1
and contract_oid = 9917997

union all

select b.row_nr
,b.contract_oid , b.rek_nr,b.rekeningsoort_nr,b.peil_dt,b.trx_journaal_dt,b.saldo_voor_rte_boeking_bg
,a.start_balance - b.betaalde_rente_bg - (a.saldo_voor_rte_boeking_bg- b.saldo_voor_rte_boeking_bg )as start_balance_NEW
,b.betaalde_rente_bg
,(start_balance_NEW + coalesce(b.betaalde_rente_bg,0)) as end_balance_NEW 
from rente_berekening as a
,s_dm_cdo.vr_rekening_transactie  as b
where  b.row_nr = a.row_nr+1
and b.contract_oid = a.contract_oid
and b.contract_oid = 9917997
);