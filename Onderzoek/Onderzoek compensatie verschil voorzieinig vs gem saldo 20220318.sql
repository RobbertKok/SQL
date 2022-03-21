select a.peil_dt,a.contract_oid, a.gem_maand_saldo_bg
,a.saldo_debetrente_bg
,a.compensatie_mnd_bg, a.cum_compensatie_bg
,a.avg_klant_rte_jaar_perc
,a.avg_referentie_rte_jaar_perc
,c.saldo, c.gecorrigeerdsaldo
,c.betaalderente, c.gecorrigeerderente
,c.rentebedragverschil
,sum(c.rentebedragverschil) OVER (partition by c.contract_oid order by c.peil_dt rows unbounded preceding ) cum_rentebedragverscil
 from  s_dm_variabelerente.rekening_compensatie_gem_saldo a
join s_dm_variabelerente.rekening_klant_segment b
on a.contract_oid = b.contract_oid
and b.persona = 'ANNA'
left join S_DM_VariabeleRente."voorziening.rente_berekening_t" c
on a.contract_oid = c.contract_oid
and a.peil_dt = c.peil_dt
where 1=1 
order by 2,1;


select a.peil_dt,a.contract_oid, a.gem_maand_saldo_bg
 c.gecorrigeerdsaldo
,sum(c.rentebedragverschil) OVER (partition by c.contract_oid order by c.peil_dt rows unbounded preceding ) cum_rentebedragverscil
 from  s_dm_variabelerente.rekening_compensatie_gem_saldo a
join s_dm_variabelerente.rekening_klant_segment b
on a.contract_oid = b.contract_oid
and b.persona = 'ANNA'
left join S_DM_VariabeleRente."voorziening.rente_berekening_t" c
on a.contract_oid = c.contract_oid
and a.peil_dt = c.peil_dt
where 1=1 
order by 2,1;
