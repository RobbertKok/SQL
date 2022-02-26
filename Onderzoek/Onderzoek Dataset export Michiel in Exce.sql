drop table s_dm_cdo.vr_uitlevering_michiel;


create table s_dm_cdo.vr_uitlevering_michiel
as
(select 

rek.contract_oid
,rek.REK_NR
,rek.REK_SRT
,rek.inp_kred_limiet 
,rek.aanvangs_rte_maand_perc
,mar.Referentie_rente*100 as referentie_rte_jaar_perc
,mar.aanvangs_rte_jaar_perc*100 as aanvangs_rte_jaar_perc
,extract(year from rek.opening_dt)*100 + extract(month from rek.opening_dt) as opening_jmnr
,extract(year from rek.opgeheven_dt)*100+ extract(month from rek.opgeheven_dt) as opgeheven_jmnr
,zeroifnull(com.compensatie_bg) as compensatie_bg
,case when com.compensatie_bg > 0 then 1 else 0 end as compensatie_ind

from s_dm_cdo.vr_rekening rek
left join
--join 
vr_marge_per_contract mar
on  rek.contract_oid = mar.contract_oid
join vr_rente_berekening_agg com
on rek.contract_oid = com.contract_oid
where rek.opening_dt > date'2006-01-01'
--where rek.contract_oid = 9917997
)with data unique primary index(contract_oid);