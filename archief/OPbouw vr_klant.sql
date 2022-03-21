drop table s_dm_cdo.vr_klant;

create table s_dm_cdo.vr_klant
as (
select distinct 
rek.partij_oid
,dk.klant_nr
,dk.klant_uitstroom_dt
,dk.vervallen_klant_ind as vervallen_klant_ind_dk 
--,sk.vervallen_klant_ind as vervallen_klant_ind_sk
from s_dm_cdo.vr_rekening rek
left join dm_ster.dim_klant dk
on rek.partij_oid = dk.partij_oid
and dk.dm_eind_dt = '9999-12-31'
--left join dwh.sas_klant sk
--on rek.partij_oid = sk.partij_oid
--and sk.eind_dt = '9999-12-31'
--and sk.geldig_ind = 1

)with data unique primary index(partij_oid);



drop table s_dm_cdo.klant;


create table s_dm_cdo.klant
as
(
select   distinct
         cpl.partij_oid
		,zeroifnull(dk.vervallen_klant_ind) as vervallen_klant_ind
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
 		   
) with data unique primary index(partij_oid);