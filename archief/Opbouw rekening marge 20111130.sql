drop TABLE s_dm_cdo.vr_marge_per_contract;

CREATE TABLE s_dm_cdo.vr_marge_per_contract AS
(
select distinct r.contract_oid, r.opening_dt,laatste_dag_vd_maand
,rr.referentie_rente
,r.aanvangs_rte_maand_perc*12/100 as aanvangs_rte_jaar_perc
,cast((aanvangs_rte_jaar_perc - rr.referentie_rente ) as decimal(32,16)) AS Marge
from s_dm_cdo.vr_rekening r
join dm_ster.dim_datum dd
on r.opening_dt = dd.dim_datum_id
left join s_dm_cdo.vr_referentie_rente rr 
on dd.laatste_dag_vd_maand = rr.maand
) with data unique primary index(contract_oid)
;


