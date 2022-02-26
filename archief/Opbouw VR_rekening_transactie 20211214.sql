
/* Nieuwe code Rutger code met rente betalingen die vanaf andere rekening wordt geincasseerd */



drop table s_dm_cdo.vr_rekening_transactie;
------
create table s_dm_cdo.vr_rekening_transactie
as
(select
row_number() over (partition by rek.contract_oid order by peil_dt ) as row_nr
,row_number() over (partition by rek.contract_oid order by peil_dt desc ) as row_nr_desc
,rek.contract_oid
,rek.rek_nr
,trx.rekeningsoort_nr
,trx.tegenrekening_nr
,ultimo_maand_dt_vor_mnd as peil_dt
,cast(trx.journaal_dt-19000000 as date) as trx_journaal_dt
,CASE WHEN trx.transactie_bg = 0 AND row_nr_desc <> 1 THEN trx2.transactie_bg
ELSE trx.transactie_bg END as betaalde_rente_bg
--,(trx.transactiegroep_cd*1000 + trx.transactiesubgroep_cd*100+trx.transactiesoort_cd) as transactiesoort
,trx.vorig_contract_saldo_bg as saldo_voor_rte_boeking_bg 
from 
(select dim_datum_id
,datum_jjjjmmdd
,laatste_dag_vd_maand as ultimo_maand_dt
,min(laatste_dag_vd_maand) over (order by dim_datum.datum_jjjjmmdd rows between 2 preceding and 1 preceding) as ultimo_maand_dt_vor_mnd
from dm_ster.dim_datum
where jaar_vd_kalender >= 2003 and kalender_datum < date
) kal
left join dwh.sas_rekening_transactie trx
on kal.datum_jjjjmmdd = trx.journaal_dt
left join dwh.sas_rekening_transactie trx2
on trx.journaal_dt = trx2.journaal_dt
and trx.tegenrekening_nr = trx2.rekening_nr
and trx2.tegenrekening_nr = trx.rekening_nr
and (trx2.transactiegroep_cd*1000 + trx2.transactiesubgroep_cd*100+trx2.transactiesoort_cd) = 7602
and trx.transactie_bg = 0
and trx2.journaal_dt >= 20030101
join s_dm_cdo.vr_rekening rek
on rek.contract_oid = trx.contract_oid
and trx.journaal_dt >= 20030101
and (trx.transactiegroep_cd*1000 + trx.transactiesubgroep_cd*100+trx.transactiesoort_cd) = 7600)
with data unique primary index(contract_oid,trx_journaal_dt)
;


/* Oude code */



drop table s_dm_cdo.vr_rekening_transactie;

create table s_dm_cdo.vr_rekening_transactie
as
(
select
 row_number() over (partition by rek.contract_oid order by peil_dt ) as row_nr
,rek.contract_oid
,rek.rek_nr
,trx.rekeningsoort_nr
,ultimo_maand_dt_vor_mnd as peil_dt
,cast(trx.journaal_dt-19000000 as date) as trx_journaal_dt
,case when trx.transactie_bg = 0 then trx2.transactie_bg else trx.transactie_bg end as betaalde_rente_bg
--,(trx.transactiegroep_cd*1000 + trx.transactiesubgroep_cd*100+trx.transactiesoort_cd) as transactiesoort
,trx.vorig_contract_saldo_bg as saldo_voor_rte_boeking_bg

from (select dim_datum_id
,datum_jjjjmmdd
,laatste_dag_vd_maand as ultimo_maand_dt
,min(laatste_dag_vd_maand) over (order by dim_datum.datum_jjjjmmdd rows between 2 preceding and 1 preceding) as ultimo_maand_dt_vor_mnd
from dm_ster.dim_datum
where jaar_vd_kalender >= 2003 and kalender_datum < date
) kal
left join dwh.sas_rekening_transactie trx
on kal.datum_jjjjmmdd = trx.journaal_dt
join s_dm_cdo.vr_rekening rek
on rek.contract_oid = trx.contract_oid
and trx.journaal_dt >= 20030101
and (trx.transactiegroep_cd*1000 + trx.transactiesubgroep_cd*100+trx.transactiesoort_cd) = 7600

left join dwh.sas_rekening_transactie trx2
on trx.tegenrekening_nr = trx2.rekening_nr
and trx2.tegenrekening_nr = trx.rekening_nr
and trx.journaal_dt = trx2.journaal_dt
and (trx2.transactiegroep_cd*1000 + trx2.transactiesubgroep_cd*100+trx2.transactiesoort_cd) = 7602
--where rek.contract_oid = 9917997
)with data unique primary index(contract_oid,trx_journaal_dt)
;


