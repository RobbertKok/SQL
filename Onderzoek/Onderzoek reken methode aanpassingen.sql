sel row_number() over (partition by trx.contract_oid order by trx.journaal_dt,volg_nr ) as row_nr
,trx.journaal_dt
,transactie_bg
,vorig_contract_saldo_bg
,(trx.transactiegroep_cd*1000 + trx.transactiesubgroep_cd*100+trx.transactiesoort_cd) as transactie_srt
,def.transactiesoort_naam
,case when transactie_srt = 7600 then 1 else 0 end as rente_betaling

from dwh.sas_rekening_transactie trx
left join dwh.sas_transactiedefinitie def
on trx.bank_nr = def.bank_nr
and def.transactietype_nr = 'FR'
and (def.transactiegroep_cd*1000 + def.transactiesubgroep_nr*100+def.transactiesoort_cd) = (trx.transactiegroep_cd*1000 + trx.transactiesubgroep_cd*100+trx.transactiesoort_cd)
where contract_oid = 9397068
order by row_nr;