
/* Onderzoek betaalde rente / gemiddeld maandsaldo om klant_rte te bepalen */

drop table s_dm_cdo.VR_gem_saldo;

create table s_dm_cdo.VR_gem_saldo
as
(
select contract_oid
,laatste_dag_vd_maand  as peil_dt
,sum(saldo_valuta_dt*dagen_saldo_tussen_maandstanden) as teller
,sum(dagen_saldo_tussen_maandstanden) as noemer
,teller/noemer as gem_maand_saldo_bg
from 
		(
		select
		row_nr
		,contract_oid
		,rekening_nr
		,rekeningsoort_nr
		,valuta_dtd as saldo_datum_vanaf
		,laatste_dag_vd_maand 
		,max(valuta_dtd) OVER (partition by contract_oid order by valuta_dtd rows between 1 following  and 1 following) -1 as saldo_datum_totenmet  
		--,volg_nr
		--,transactie_bg
		--,vorig_contract_saldo_bg
		,saldo_valuta_dt
		--,transactie_srt
		--,transactiesoort_naam
		--,rente_betaling
		,case when saldo_datum_totenmet > (laatste_dag_vd_maand -1)
			 then laatste_dag_vd_maand - valuta_dtd + 1
			 else saldo_datum_totenmet - valuta_dtd +1
		end as dagen_saldo_tussen_maandstanden     

				from (sel row_number() over (partition by trx.contract_oid order by trx.journaal_dt,volg_nr ) as row_nr
						,rek.contract_oid
						,trx.rekening_nr
						,trx.rekeningsoort_nr
						,cast(trx.valuta_dt-19000000 as date) as valuta_dtd
						,trx.volg_nr
						,transactie_bg
						,vorig_contract_saldo_bg
						,(vorig_contract_saldo_bg + transactie_bg) as saldo_valuta_dt
						,(trx.transactiegroep_cd*1000 + trx.transactiesubgroep_cd*100+trx.transactiesoort_cd) as transactie_srt
						,def.transactiesoort_naam
						,case when transactie_srt = 7600 then 1 else 0 end as rente_betaling
						,case when transactie_srt = 7600 then transactie_bg else 0 end as betaald_rente_bg

						from dwh.sas_rekening_transactie trx
						join s_dm_cdo.vr_rekening rek
						on trx.contract_oid = rek.contract_oid
						left join dwh.sas_transactiedefinitie def
						on (def.transactiegroep_cd*1000 + def.transactiesubgroep_nr*100+def.transactiesoort_cd) = (trx.transactiegroep_cd*1000 + trx.transactiesubgroep_cd*100+trx.transactiesoort_cd)
						and def.bank_nr = 0
						and def.transactietype_nr = 'FR'
						and def.eind_dt = '9999-12-31' and def.geldig_ind = 1
						where rek.contract_oid  = 461191204

						qualify row_number() over (partition by trx.contract_oid,valuta_dtd order by volg_nr desc) = 1 
						) s
		join dm_ster.dim_datum dd
		on s.valuta_dtd = dd.dim_datum_id 
		and dim_datum_id between date'2006-01-01' and date
		   )s2
group by 1,2
)with data unique primary index(contract_oid, peil_dt);


-- hier was ik mee bezig: Het geneste stuk van de code hierboven

select
		select
row_nr
		,contract_oid
		--,rekening_nr
		,rekeningsoort_nr
		,trx_valuta_dt as saldo_datum_vanaf
		,laatste_dag_vd_maand 
		,max(trx_valuta_dt) OVER (partition by contract_oid order by trx_valuta_dt rows between 1 following  and 1 following) -1 as saldo_datum_totenmet  
		--,volg_nr
		--,transactie_bg
		--,vorig_contract_saldo_bg
		,saldo_valuta_dt
		--,transactie_srt
		--,transactiesoort_naam
		--,rente_betaling
		,case when saldo_datum_totenmet > (laatste_dag_vd_maand -1)
			 then laatste_dag_vd_maand - trx_valuta_dt + 1
			 else saldo_datum_totenmet - trx_valuta_dt +1
		end as dagen_saldo_tussen_maandstanden     
       ,max(dd.aantal_dagen_tot_verval_rte_dag) OVER (partition by contract_oid,trx_valuta_dt/100 order by trx_valuta_dt rows between 1 following  and 1 following) - dd.aantal_dagen_tot_verval_rte_dag 
	   ,dd.aantal_dagen_tot_verval_rte_dag
	   
				from (sel row_number() over (partition by trx.contract_oid order by trx.trx_valuta_dt,volg_nr ) as row_nr
						,rek.contract_oid
						--,trx.rekening_nr
						,trx.rekeningsoort_nr
						,trx.trx_valuta_dt as trx_valuta_dt
						,trx.volg_nr
						,transactie_bg
						,vorig_contract_saldo_bg
						,(vorig_contract_saldo_bg + transactie_bg) as saldo_valuta_dt
					--	,(trx.transactiegroep_cd*1000 + trx.transactiesubgroep_cd*100+trx.transactiesoort_cd) as transactie_srt
					--	,def.transactiesoort_naam
					--	,case when transactie_srt = 7600 then 1 else 0 end as rente_betaling
					--	,case when transactie_srt = 7600 then transactie_bg else 0 end as betaald_rente_bg

						from s_dm_cdo.rentepot_transacties trx
						join s_dm_cdo.vr_rekening rek
						on trx.contract_oid = rek.contract_oid
						/*left join dwh.sas_transactiedefinitie def
						on (def.transactiegroep_cd*1000 + def.transactiesubgroep_nr*100+def.transactiesoort_cd) = (trx.transactiegroep_cd*1000 + trx.transactiesubgroep_cd*100+trx.transactiesoort_cd)
						and def.bank_nr = 0
						and def.transactietype_nr = 'FR'
						and def.eind_dt = '9999-12-31' and def.geldig_ind = 1
						*/
						where rek.contract_oid  = 461191204

						qualify row_number() over (partition by trx.contract_oid,trx_valuta_dt order by volg_nr desc) = 1 
						) s
		join (select dim_datum_id,laatste_dag_vd_maand,dag_vd_maand
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
		on s.trx_valuta_dt = dd.dim_datum_id 
		and dim_datum_id between date'2006-01-01' and date
		order by 1