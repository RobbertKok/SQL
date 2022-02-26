drop table s_dm_cdo.VR_gem_saldo_1;
create table s_dm_cdo.VR_gem_saldo_1
as
(
sel row_number() over (partition by trx.contract_oid order by trx.journaal_dt,volg_nr ) as row_nr
,trx.contract_oid
,trx.rekening_nr
,trx.rekeningsoort_nr
,cast(trx.journaal_dt-19000000 as date) as journaal_dtd
,trx.volg_nr
,transactie_bg
,vorig_contract_saldo_bg
,(vorig_contract_saldo_bg + transactie_bg) as saldo_journaal_dt
,(trx.transactiegroep_cd*1000 + trx.transactiesubgroep_cd*100+trx.transactiesoort_cd) as transactie_srt
,def.transactiesoort_naam
,case when transactie_srt = 7600 then 1 else 0 end as rente_betaling

from dwh.sas_rekening_transactie trx
join s_dm_cdo.vr_rekening r
on trx.contract_oid = r.contract_oid
left join dwh.sas_transactiedefinitie def
on (def.transactiegroep_cd*1000 + def.transactiesubgroep_nr*100+def.transactiesoort_cd) = (trx.transactiegroep_cd*1000 + trx.transactiesubgroep_cd*100+trx.transactiesoort_cd)
and def.bank_nr = 0
and def.transactietype_nr = 'FR'
and def.eind_dt = '9999-12-31' and def.geldig_ind = 1
--where contract_oid  = 23234749
qualify row_number() over (partition by trx.contract_oid,journaal_dtd order by volg_nr desc) = 1
) with data primary index(contract_oid) ;

drop table s_dm_cdo.VR_gem_saldo_2;

create table s_dm_cdo.VR_gem_saldo_2
as(
select
row_nr
,contract_oid
,rekening_nr
,rekeningsoort_nr
,journaal_dtd as saldo_datum_vanaf
,laatste_dag_vd_maand 
,max(journaal_dtd) OVER (partition by contract_oid order by journaal_dtd rows between 1 following  and 1 following) -1 as saldo_datum_totenmet  
--,volg_nr
--,transactie_bg
--,vorig_contract_saldo_bg
,saldo_journaal_dt
--,transactie_srt
--,transactiesoort_naam
,rente_betaling
,case when saldo_datum_totenmet > (laatste_dag_vd_maand -1)
     then laatste_dag_vd_maand - journaal_dtd + 1
     else saldo_datum_totenmet - journaal_dtd +1
end as dagen_saldo_tussen_maandstanden     

from s_dm_cdo.VR_gem_saldo_1 s
join dm_ster.dim_datum dd
on s.journaal_dtd = dd.dim_datum_id 
and dim_datum_id between date'2006-01-01' and date
)with data unique primary index( contract_oid, saldo_datum_vanaf) 




/* Totaal alles in 1 keer in 1 tabel */

drop table s_dm_cdo.VR_gem_saldo;


create table s_dm_cdo.VR_gem_saldo
as
(
select contract_oid
,laatste_dag_vd_maand  as peil_dt
,sum(saldo_journaal_dt*dagen_saldo_tussen_maandstanden) as teller
,sum(dagen_saldo_tussen_maandstanden) as noemer
,teller/noemer as gem_maand_saldo_bg
from 
		(
		select
		row_nr
		,contract_oid
		,rekening_nr
		,rekeningsoort_nr
		,journaal_dtd as saldo_datum_vanaf
		,laatste_dag_vd_maand 
		,max(journaal_dtd) OVER (partition by contract_oid order by journaal_dtd rows between 1 following  and 1 following) -1 as saldo_datum_totenmet  
		--,volg_nr
		--,transactie_bg
		--,vorig_contract_saldo_bg
		,saldo_journaal_dt
		--,transactie_srt
		--,transactiesoort_naam
		--,rente_betaling
		,case when saldo_datum_totenmet > (laatste_dag_vd_maand -1)
			 then laatste_dag_vd_maand - journaal_dtd + 1
			 else saldo_datum_totenmet - journaal_dtd +1
		end as dagen_saldo_tussen_maandstanden     

				from (sel row_number() over (partition by trx.contract_oid order by trx.journaal_dt,volg_nr ) as row_nr
						,rek.contract_oid
						,trx.rekening_nr
						,trx.rekeningsoort_nr
						,cast(trx.journaal_dt-19000000 as date) as journaal_dtd
						,trx.volg_nr
						,transactie_bg
						,vorig_contract_saldo_bg
						,(vorig_contract_saldo_bg + transactie_bg) as saldo_journaal_dt
						,(trx.transactiegroep_cd*1000 + trx.transactiesubgroep_cd*100+trx.transactiesoort_cd) as transactie_srt
						,def.transactiesoort_naam
						,case when transactie_srt = 7600 then 1 else 0 end as rente_betaling

						from dwh.sas_rekening_transactie trx
						join s_dm_cdo.vr_rekening rek
						on trx.contract_oid = rek.contract_oid
						left join dwh.sas_transactiedefinitie def
						on (def.transactiegroep_cd*1000 + def.transactiesubgroep_nr*100+def.transactiesoort_cd) = (trx.transactiegroep_cd*1000 + trx.transactiesubgroep_cd*100+trx.transactiesoort_cd)
						and def.bank_nr = 0
						and def.transactietype_nr = 'FR'
						and def.eind_dt = '9999-12-31' and def.geldig_ind = 1
						--where contract_oid  = 23234749
						qualify row_number() over (partition by trx.contract_oid,journaal_dtd order by volg_nr desc) = 1 
						) s
		join dm_ster.dim_datum dd
		on s.journaal_dtd = dd.dim_datum_id 
		and dim_datum_id between date'2006-01-01' and date
		   )s2
group by 1,2
)with data unique primary index(contract_oid, peil_dt);



/* combineatie met referetie rente */
sel t.*
,r.referentie_rente (decimal(18,4)) as referentie_jaar_rente
,r.Referentie_mnd_rente (decimal(18,4))
from s_dm_cdo.VR_gem_saldo_2 t
join S_DM_CDO.vr_referentie_rente r
on  t.laatste_dag_vd_maand = r.maand  
order by 2,1