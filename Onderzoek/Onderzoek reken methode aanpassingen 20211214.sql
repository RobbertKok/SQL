sel row_number() over (partition by trx.contract_oid order by trx.journaal_dt,volg_nr ) as row_nr
,contract_oid
,trx.rekening_nr
,trx.rekeningsoort_nr
,cast(trx.journaal_dt-19000000 as date) as journaal_dtd
,transactie_bg
,vorig_contract_saldo_bg
,(vorig_contract_saldo_bg + transactie_bg) as saldo_journaal_dt
,(trx.transactiegroep_cd*1000 + trx.transactiesubgroep_cd*100+trx.transactiesoort_cd) as transactie_srt
,def.transactiesoort_naam
,case when transactie_srt = 7600 then 1 else 0 end as rente_betaling

from dwh.sas_rekening_transactie trx
left join dwh.sas_transactiedefinitie def
on (def.transactiegroep_cd*1000 + def.transactiesubgroep_nr*100+def.transactiesoort_cd) = (trx.transactiegroep_cd*1000 + trx.transactiesubgroep_cd*100+trx.transactiesoort_cd)
and def.bank_nr = 0
and def.transactietype_nr = 'FR'
and def.eind_dt = '9999-12-31' and def.geldig_ind = 1
where contract_oid  = 23234749
order by row_nr; 





drop table vr_onderzoek_geen_rte_trx;

create table vr_onderzoek_geen_rte_trx
as
(
sel row_number() over (partition by trx.contract_oid order by trx.journaal_dt,volg_nr ) as row_nr
,trx.contract_oid
,trx.rekening_nr
,trx.rekeningsoort_nr
,trx.journaal_dt
,transactie_bg
,vorig_contract_saldo_bg
,(trx.transactiegroep_cd*1000 + trx.transactiesubgroep_cd*100+trx.transactiesoort_cd) as transactie_srt
,def.transactiesoort_naam
,case when transactie_srt = 7600 then 1 else 0 end as rente_betaling

from dwh.sas_rekening_transactie trx
left join dwh.sas_transactiedefinitie def
on (def.transactiegroep_cd*1000 + def.transactiesubgroep_nr*100+def.transactiesoort_cd) = (trx.transactiegroep_cd*1000 + trx.transactiesubgroep_cd*100+trx.transactiesoort_cd)
and def.bank_nr = 0
and def.transactietype_nr = 'FR'
and def.eind_dt = '9999-12-31' and def.geldig_ind = 1
join    (select a.contract_oid, opening_dt 
   from s_dm_cdo.vr_rekening a
   left join s_dm_cdo.vr_rente_berekening_t t 
   on a.contract_oid = t.contract_oid
   where t.contract_oid is null
   and a.opening_dt> date'2006-01-01'
   and  case	when ZEROIFNULL(aanvangs_rte_maand_perc) = 0 or aanvangs_rte_maand_perc= 0 then  0 -- '(nog) niet bekend'
				when aanvangs_rte_maand_perc < 0.0 or aanvangs_rte_maand_perc> 1.5 then 0        --'dubieus gevuld'
				ELSE 1 --'maandrente aanwezig'
  		END = 1
    --and a.contract_oid = 155607887
   )rek 
on trx.contract_oid = rek.contract_oid 
where cast(trx.journaal_dt -19000000 as date) >= rek.opening_dt
   
)with data unique primary index(contract_oid, row_nr);
