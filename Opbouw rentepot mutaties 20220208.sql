/* Basistabel van klantsegment met daarin alle financiele transacties in een bepaalde periode 	*/
/* 20220201: Robbert Kok																		*/referentie_rte_incl_marge_jaar_perc 
/* 																								*/
/* 																								*/
/* 																								*/


 

drop table s_dm_cdo.rentepot_transacties;

create table s_dm_cdo.rentepot_transacties
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
join s_dm_cdo.rekening_klant_segment anna               --ALLEEN de persona ANNA : per groep draaien
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


/* Tabel van klantsegment met daarin alle financiele transacties en rente wissel mutaties in een bepaalde periode 	*/
/* 20220201: Robbert Kok																							*/
/* 																													*/
/* 																													*/
/* 																													*/


drop table s_dm_cdo.rentepot_mutaties;

create table s_dm_cdo.rentepot_mutaties
as(
select 
row_number() over (partition by basetable.contract_oid order by  basetable.dim_datum_id, volg_nr,rte_wissel) as row_nr
,coalesce(t1.contract_oid,trx.contract_oid) as contract_oid
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
,t1.klant_rte_jaar_perc
,t1.referentie_rte_incl_marge_jaar_perc
,trx.trx_valuta_dt
,trx.volg_nr
,trx.transactiesoort
,trx.VORIG_CONTRACT_SALDO_BG
,trx.transactie_bg
,(trx.transactie_bg+ trx.VORIG_CONTRACT_SALDO_BG) as HUIDIG_CONTRACT_SALDO_BG
-- berekening rente
,basetable.aantal_dagen_tot_verval_rte_dag
-- renteberekening voor betaalde rente
-- de rentepot mutatie wordt positief gevuld bij rente betalen en negatief bij rente correcties
, case when rente_afreken_periode_cd = 6 and rente_berekening_methode_cd = 1						-- maand rente kapitalisatie
       then cast((aantal_dagen_tot_verval_rte_dag * t1.klant_rte_jaar_perc) as decimal(18,6))/36000 
	   when rente_afreken_periode_cd = 9 and rente_berekening_methode_cd = 1						--jaarrente kapitalisatie
	   then cast(((basetable.laatste_dag_vh_jaar - trx.trx_valuta_dt) * t1.klant_rte_jaar_perc) as decimal(18,6))/36000 
  end	   
	   as rentefactor   

,cast((case when transactiesoort = 7600 then huidig_contract_saldo_bg * rentefactor*-1            -- maand rente kapitalisatie
        when rte_wissel = 1 then saldo_bg * (rentefactor) * -1
else 0 end) as decimal(18,2)) as rente_pot_start										-- de rentepot wordt positief gevuld 

,cast((case when transactiesoort <> 7600  then zeroifnull(transactie_bg) * rentefactor*-1           -- maand rente kapitalisatie
when rte_wissel = 1 then (1*saldo_bg * (cast( aantal_dagen_tot_verval_rte_dag *(sum(klant_rte_jaar_perc) over (partition by basetable.contract_oid order by basetable.dim_datum_id rows between 1 preceding and 1 preceding)) as decimal(18,4))/36000))  --correctie voor oude rte
else 0 end) as decimal(18,2)) as rente_pot_mutatie          


                           
--rente berekening voor referentie rente

,cast((aantal_dagen_tot_verval_rte_dag * t1.referentie_rte_incl_marge_jaar_perc)  as decimal(18,6)) /36000  as rentefactor_ref

,cast((case when transactiesoort = 7600 then huidig_contract_saldo_bg * rentefactor_ref*-1
        when rte_wissel = 1 then saldo_bg * rentefactor_ref* -1
else 0 end) as decimal(18,2)) as rente_pot_start_ref										-- de rentepot wordt positief gevuld 

,cast(( case when transactiesoort <> 7600  then zeroifnull(transactie_bg) * rentefactor_ref*-1
when rte_wissel = 1 then (1*saldo_bg * (cast( aantal_dagen_tot_verval_rte_dag *(sum(referentie_rte_incl_marge_jaar_perc) over (partition by basetable.contract_oid order by basetable.dim_datum_id rows between 1 preceding and 1 preceding)) as decimal(18,4))/36000))   --correctie voor oude rte
else 0 end) as decimal(18,2)) as rente_pot_mutatie_ref                                     -- de rentepot mutatie wordt positief gevuld bij rente betalen en negatief bij rente correcties


from (select contract_oid
,dim_datum_id,laatste_dag_vd_maand,dag_vd_maand,laatste_dag_vh_jaar,aantal_dagen_tot_verval_rte_dag
from 
(select distinct contract_oid
from s_dm_cdo.rentepot_transacties
union 
select distinct contract_oid
from s_dm_cdo.rekening_dagbasis 
)c
cross  join  (select dim_datum_id,laatste_dag_vd_maand,dag_vd_maand,laatste_dag_vh_jaar									-- 30/360 renteconventie toepassen op kalender datums
				,case when dag_vd_maand = 31 then 1
				        when maand_vh_jaar in (1,3,5,7,8,10,12) then laatste_dag_vd_maand - dim_datum_id
				        when maand_vh_jaar in (4,6,9,11) then  (laatste_dag_vd_maand - dim_datum_id)+1
				        when maand_vh_jaar = 2 and schrikkeljaar_ind = 0 then (laatste_dag_vd_maand - dim_datum_id)+3
				        when maand_vh_jaar = 2 and schrikkeljaar_ind = 1 then (laatste_dag_vd_maand - dim_datum_id)+2
				 end    as aantal_dagen_tot_verval_rte_dag
				,schrikkeljaar_ind 
              from  dm_ster.dim_datum dd 
              where 1=1 
              and dim_datum_id between date'2006-12-19' and date) dd
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
--,sum(klant_rte_jaar_perc) over (partition by contract_oid order by peil_dag_dt rows between 1 preceding and 1 preceding) as vorige_record_rte	
--,sum(inp_kred_limiet) over (partition by contract_oid order by peil_dag_dt rows between 1 preceding and 1 preceding) as vorige_record_limiet
,case when sum(klant_rte_jaar_perc) over (partition by contract_oid order by peil_dag_dt rows between 1 preceding and 1 preceding) <> klant_rte_jaar_perc then 1 else 0  end as rte_wissel
,case when sum(inp_kred_limiet) over (partition by contract_oid order by peil_dag_dt rows between 1 preceding and 1 preceding)  <> inp_kred_limiet then 1 else 0 end as lim_wissel
,klant_rte_jaar_perc
,referentie_rte_incl_marge_jaar_perc


from s_dm_cdo.rekening_dagbasis dst

where 1=1 
--and contract_oid = 461191204
--qualify sum(klant_rte_jaar_perc) over (partition by contract_oid order by peil_dag_dt rows between 1 preceding and 1 preceding) <> klant_rte_jaar_perc
--or sum(inp_kred_limiet) over (partition by contract_oid order by peil_dag_dt rows between 1 preceding and 1 preceding)  <> inp_kred_limiet
--or dag_vd_maand = 1
)T1
on t1.peil_dag_dt = basetable.dim_datum_id
and t1.contract_oid = basetable.contract_oid
--and t1.contract_oid = 461191204
left join s_dm_cdo.rentepot_transacties trx	 
on trx.contract_oid = basetable.contract_oid
and trx.trx_valuta_dt = basetable.dim_datum_id
--and trx.contract_oid = 461191204
where  (trx.contract_oid > 0 
or (t1.dag_vd_maand = 1  or t1.rte_wissel = 1 or lim_wissel = 1))
AND rente_afreken_periode_cd = 6 and rente_berekening_methode_cd = 1		-- PRODUCTEN MET: maandelijkse rente kapitalisatie obv 30/360

)with data primary index(contract_oid,dim_datum_id,row_nr)
;

 