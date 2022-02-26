drop table s_dm_cdo.VR_tijdreeks_dk_vb_rente;

create table s_dm_cdo.VR_tijdreeks_dk_vb_rente
as(
select
 bnk_nr as bank_nr
,rek_srt
,dim_datum_id as peil_dt
,pi.deb_rte_per 
,pi.dlk_deb_perc_1
,pi.dlk_kred_grens_1
,pi.dlk_deb_perc_2
,pi.dlk_kred_grens_2
,pi.dlk_deb_perc_3
,pi.dlk_kred_grens_3
,pi.dlk_deb_perc_4
,pi.dlk_kred_grens_4
,pi.dlk_deb_perc_5
,pi.dlk_kred_grens_5
,pi.dat_effektief 
,pi.dat_afloop 
from edv_sas.bpre_type5 			pi 
join dm_ster.dim_datum dd
on pi.pre_grp=5
and pi.geldig_ind = 1
and pi.dat_effektief <= dd.datum_jjjjmmdd 
and pi.dat_afloop  > dd.datum_jjjjmmdd 
and pi.dwh_begin_dt <= dd.dim_datum_id
and pi.dwh_eind_dt > dd.dim_datum_id
where dd.ultimo_maand_ind = 1
and dim_datum_id between date'2006-12-19' and date
where rek_srt in (597,598,599,603,606,607,608,609,615,623,624,625,638,692,695,697,824,831,839,840,866,972,980,982)
--qualify row_number() over (partition by bank_nr,rek_srt, peil_dt order by dat_effektief desc) = 1
)with data unique primary index(bank_nr,rek_srt,peil_dt)
;
