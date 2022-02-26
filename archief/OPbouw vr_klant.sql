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



