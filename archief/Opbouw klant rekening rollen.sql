drop table s_dm_cdo.klant_rekening_rol;

create table s_dm_cdo.klant_rekening_rol
as
(

select cpl.contract_oid	
        ,cpl.contract_partij_rol_cd
		,d.decode_omschr
		  ,cpl.partij_oid
		,case when cpl.relatie_eind_dt is null then 'actieve relatie' else 
			  'laatst bekende klant contract relatie' end as relatie_omschr
			  ,cpl.begin_dt as relatie_begin_dt
				
		
from s_dm_variabelerente.rekening rek
join dwh.contract_partij_l cpl
on rek.contract_oid = cpl.contract_oid
join dwh.decode d
on cpl.contract_partij_rol_cd_oid = d.decode_oid
and d.eind_dt is null
left join dm_ster.dim_klant dk
on cpl.partij_oid = dk.partij_oid
and dk.dm_eind_dt = '9999-12-31'
where cpl.eind_dt = '9999-12-31' 		-- laatst bekende partij
--and geldig_ind = 1
--and relatie_eind_dt is null
and cpl.contract_partij_rol_cd in (1,2,3,4,10,110)
 		   
) with data unique primary index(contract_oid, partij_oid,contract_partij_rol_cd)
		   

drop table s_dm_cdo.klant

create table s_dm_cdo.klant
as
(
select   distinct
         cpl.partij_oid
		,dk.vervallen_klant_ind
		,dk.klant_uitstroom_dt
		,dk.klant_heeft_betalen_ind
		,dk.klant_heeft_act_betaalrek_ind
		,leeftijdsklasse
from s_dm_variabelerente.rekening rek
join dwh.contract_partij_l cpl
on rek.contract_oid = cpl.contract_oid
join dwh.decode d
on cpl.contract_partij_rol_cd_oid = d.decode_oid
and d.eind_dt is null
left join dm_ster.dim_klant dk
on cpl.partij_oid = dk.partij_oid
and dk.dm_eind_dt = '9999-12-31'
where cpl.eind_dt = '9999-12-31' 		-- laatst bekende partij
--and geldig_ind = 1
--and relatie_eind_dt is null
and cpl.contract_partij_rol_cd in (1,2,3,10,110)
 		   
) with data unique primary index(partij_oid)
		   

		   
		   