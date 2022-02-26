drop table s_dm_cdo.rente_berekening_transacties;

create table s_dm_cdo.rente_berekening_transacties
as(
sel
trx.bank_nr
,trx.rekeningsoort_nr
,trx.contract_oid
,cast(trx.journaal_dt-19000000 as date) as trx_journaal_dt
,cast(trx.valuta_dt-19000000 as date) as trx_valuta_dt
,trx.volg_nr
,(trx.transactiegroep_cd*1000 + trx.transactiesubgroep_cd*100+trx.transactiesoort_cd) as transactiesoort
,trx.transactie_bg
,trx.VORIG_CONTRACT_SALDO_BG
,ultimo_maand_dt
,dag_vd_maand
from dwh.sas_rekening_transactie trx 
join s_dm_cdo.rekening_anna anna               --ALLEEN de persona ANNA : per groep draaien
on trx.contract_oid = anna.contract_oid
join (select dim_datum_id
,datum_jjjjmmdd
,laatste_dag_vd_maand as ultimo_maand_dt
,dag_vd_maand
from dm_ster.dim_datum
where kalender_datum >= '2006-12-19' and kalender_datum < date
)dd
on trx_journaal_dt = dd.dim_datum_id
--where contract_oid = 198461483    --TEST
)with data primary index(contract_oid)
;



select 
row_number() over (partition by basetable.contract_oid order by  basetable.dim_datum_id, volg_nr,rte_wissel) as row_nr
,coalesce(t1.contract_oid,trx.contract_oid)
,basetable.dim_datum_id
,t1.peil_dag_dt
,t1.dag_vd_maand
,basetable.laatste_dag_vd_maand
--,inp_kred_limiet
,t1.rente_afreken_periode_cd
,t1.rente_afreken_periode
,t1.rente_berekening_methode_cd
,t1.rente_berekening_methode
,t1.saldo_bg
,t1.rte_wissel
,t1.lim_wissel
,t1.rte_incl_indiv_toeslag_maand_perc
,t1.referentie_rte_maand_perc
,trx.trx_valuta_dt
,trx.volg_nr
,trx.transactiesoort
,trx.transactie_bg
,trx.VORIG_CONTRACT_SALDO_BG
,(trx.transactie_bg+ trx.VORIG_CONTRACT_SALDO_BG) as HUIDIG_CONTRACT_SALDO_BG
-- berekening rente
,(basetable.laatste_dag_vd_maand - basetable.dim_datum_id) as aantal_dagen_tot_verval_rte_dag
-- renteberekening voor betaalde rente
,(aantal_dagen_tot_verval_rte_dag * (t1.rte_incl_indiv_toeslag_maand_perc*12) /36000)  (decimal(18,4) )as rentefactor
,case when transactiesoort = 7600 then huidig_contract_saldo_bg * rentefactor*-1
        when rte_wissel = 1 then saldo_bg * rentefactor* -1
else 0 end as rente_pot_start										-- de rentepot wordt positief gevuld 
,case when transactiesoort <> 7600  then zeroifnull(transactie_bg) * rentefactor*-1
when rte_wissel = 1 then (1*saldo_bg * ((sum(rte_incl_indiv_toeslag_maand_perc) over (partition by basetable.contract_oid order by basetable.dim_datum_id rows between 1 preceding and 1 preceding)*12)/36000)*aantal_dagen_tot_verval_rte_dag)  --correctie voor oude rte
else 0 end as rente_pot_mutatie                                     -- de rentepot mutatie wordt positief gevuld bij rente betalen en negatief bij rente correcties
--rente berekening voor referentie rente
,(aantal_dagen_tot_verval_rte_dag * (t1.referentie_rte_maand_perc*12) /36000)  (decimal(18,4) )as rentefactor_ref
,case when transactiesoort = 7600 then huidig_contract_saldo_bg * rentefactor_ref*-1
        when rte_wissel = 1 then saldo_bg * rentefactor_ref* -1
else 0 end as rente_pot_start_ref										-- de rentepot wordt positief gevuld 
,case when transactiesoort <> 7600  then zeroifnull(transactie_bg) * rentefactor_ref*-1
when rte_wissel = 1 then (1*saldo_bg * ((sum(referentie_rte_maand_perc) over (partition by basetable.contract_oid order by basetable.dim_datum_id rows between 1 preceding and 1 preceding)*12)/36000)*aantal_dagen_tot_verval_rte_dag)  --correctie voor oude rte
else 0 end as rente_pot_mutatie_ref                                     -- de rentepot mutatie wordt positief gevuld bij rente betalen en negatief bij rente correcties
--,case when basetable.dim_datum_id = basetable.laatste_dag_vd_maand then sum(zeroifnull(rente_pot_start) + zeroifnull(rente_pot_mutatie)) over (partition by basetable.contract_oid,basetable.laatste_dag_vd_maand)
--else 0 end as compute_rte_betaald_bg
--,case when basetable.dim_datum_id = basetable.laatste_dag_vd_maand then sum(zeroifnull(rente_pot_start_ref) + zeroifnull(rente_pot_mutatie_ref)) over (partition by basetable.contract_oid,basetable.laatste_dag_vd_maand)
--else 0 end as compute_rte_betaald_bg_ref


from (select contract_oid
,dim_datum_id,laatste_dag_vd_maand,dag_vd_maand
from 
(select distinct contract_oid
from s_dm_cdo.rente_berekening_transacties
union 
select distinct contract_oid
from s_dm_cdo.rekening_dagbasis 
)c
cross  join  (select dim_datum_id,laatste_dag_vd_maand,dag_vd_maand from  dm_ster.dim_datum dd where dim_datum_id between date'2006-12-19' and date) dd
)basetable

left join 
(select  contract_oid
,peil_dag_dt
,dag_vd_maand
,ultimo_maand_dt
--,inp_kred_limiet
,deb_rte_per as rente_afreken_periode_cd
,rente_afreken_periode
,deb_rte_dgn as rente_berekening_methode_cd
,rente_berekening_methode
,saldo_bg
--,sum(rte_incl_indiv_toeslag_maand_perc) over (partition by contract_oid order by peil_dag_dt rows between 1 preceding and 1 preceding) as vorige_record_rte	
--,sum(inp_kred_limiet) over (partition by contract_oid order by peil_dag_dt rows between 1 preceding and 1 preceding) as vorige_record_limiet
,case when sum(rte_incl_indiv_toeslag_maand_perc) over (partition by contract_oid order by peil_dag_dt rows between 1 preceding and 1 preceding) <> rte_incl_indiv_toeslag_maand_perc then 1 else 0  end as rte_wissel
,case when sum(inp_kred_limiet) over (partition by contract_oid order by peil_dag_dt rows between 1 preceding and 1 preceding)  <> inp_kred_limiet then 1 else 0 end as lim_wissel
,rte_incl_indiv_toeslag_maand_perc
,referentie_rte_maand_perc


from s_dm_cdo.rekening_dagbasis dst

where 1=1 
and contract_oid = 461191204
--qualify sum(rte_incl_indiv_toeslag_maand_perc) over (partition by contract_oid order by peil_dag_dt rows between 1 preceding and 1 preceding) <> rte_incl_indiv_toeslag_maand_perc
--or sum(inp_kred_limiet) over (partition by contract_oid order by peil_dag_dt rows between 1 preceding and 1 preceding)  <> inp_kred_limiet
--or dag_vd_maand = 1
)T1
on t1.peil_dag_dt = basetable.dim_datum_id
and t1.contract_oid = basetable.contract_oid
and t1.contract_oid = 461191204
left join s_dm_cdo.rente_berekening_transacties trx	 
on trx.contract_oid = basetable.contract_oid
and trx.trx_valuta_dt = basetable.dim_datum_id
and trx.contract_oid = 461191204
where  trx.contract_oid > 0 
or (t1.dag_vd_maand = 1  or t1.rte_wissel = 1 or lim_wissel = 1)
order by 1,2;