

drop table s_dm_variabelerente.rekening_rekening_rol;

create table s_dm_variabelerente.rekening_rekening_rol
as(
select rek.contract_oid
,rek.rek_nr
,case when ccl.relatie_eind_dt is null then 'actieve relatie' else 
		 'laatst bekende contract contract relatie' end as relatie_omschr
,ccl. contract_rol_cd
,d.decode_omschr
,chu.bron_contract_2_nr as IBAN_nr
,chu.bron_contract_nr as tegenrek_nr
from S_DM_VariabeleRente.rekening rek
left join dwh.contract_contract_l ccl
on rek.contract_oid = ccl.relateert_aan_contract_oid
--and ccl.eind_dt = '9999-12-31' and ccl.geldig_ind = 1
--and ccl.relatie_eind_dt is null
left join dwh.contract_h chu
on ccl.gerelateerd_contract_oid = chu.contract_oid
left join dwh.decode d
on d.decode_oid = ccl.contract_rol_cd_oid
where ccl.contract_rol_cd is not null
qualify row_number() over (partition by rek.contract_oid, ccl.contract_rol_cd order by coalesce(ccl.relatie_eind_dt,date'9999-12-31') desc, ccl.relatie_begin_dt desc,ccl.geldig_ind desc) = 1
)with data unique primary index(contract_oid,contract_rol_cd)
;