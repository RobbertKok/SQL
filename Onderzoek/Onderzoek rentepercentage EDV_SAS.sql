select rk.bank_nr
,rk.rek_srt
,pr.prod_naam
,rk.rek_nr
,rk.kred_limiet
,rk.saldo
,rk.dat_opening
,case when rk.afb=1 or rk.afb_znd_boekje=1 then 'J' else 'N' end as OPGEHEVEN
,case when rk.afb=1 or rk.afb_znd_boekje=1 then dat_afb else 0 end as DAT_OPHEFFEN
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
,Case when aq.kred_limiet <> 0 then aq.kred_limiet else rk.kred_limiet end as inp_kred_limiet
,pi2.deb_perc
,case when inp_kred_limiet < pi.dlk_kred_grens_1 then pi.dlk_deb_perc_1
      when inp_kred_limiet < pi.dlk_kred_grens_2 then pi.dlk_deb_perc_2
      when inp_kred_limiet < pi.dlk_kred_grens_3 then pi.dlk_deb_perc_3
      when inp_kred_limiet < pi.dlk_kred_grens_4 then pi.dlk_deb_perc_4
      when inp_kred_limiet < pi.dlk_kred_grens_5 then pi.dlk_deb_perc_5
      else null 
 end as Maandperc_huidige_RSC_bij_afsluiten_contract
,(Maandperc_huidige_RSC_bij_afsluiten_contract + pi2.deb_perc) as Incl_indiv_op_afslag
from edv_sas.brek rk
join edv_sas.bpro pr 
on pr.bpro_oid=rk.bpro_oid 
and pr.aktueel=1 
and pr.dlk=1
and pr.dat_afloop =99999999
and rk.dwh_eind_dt is null
and pr.dwh_eind_dt is null
and pr.geldig_ind = 1
join edv_sas.bpre_type5 			pi 
on pi.bank_nr=rk.bank_nr 
and pi.bpro_oid=rk.bpro_oid 
and pi.pre_grp=5 
and pi.dat_effektief <= rk.dat_opening 
and pi.dat_afloop>rk.dat_opening
and pi.dwh_eind_dt is null
left outer join edv_sas.brcag_type1 aq
on aq.brek_oid=rk.brek_oid 
and aq.rcag_grp=1 
and aq.dat_effektief=rk.dat_opening 
and aq.dat_afloop>rk.dat_opening
and aq.dwh_eind_dt is null
and aq.geldig_ind = 1
left outer join edv_sas.bpre_type1 pi2 
on pi2.brek_oid=rk.brek_oid 
--and pi2.rek_nr=rk.rek_nr
and pi2.pre_grp=1 
and pi2.dat_effektief=rk.dat_opening 
and pi2.dat_afloop>pi2.dat_effektief
and pi2.dwh_eind_dt is null
and pi2.geldig_ind = 1
where rk.rek_nr = 855455888
order by rk.bank_nr,rk.rek_srt,rk.rek_nr;

/* produckten met lege rente percentages BB */

select rk.bank_nr
,rk.rek_srt
,pr.prod_naam
,rk.rek_nr
,rk.kred_limiet
,rk.saldo
,rk.dat_opening
,case when rk.afb=1 or rk.afb_znd_boekje=1 then 'J' else 'N' end as OPGEHEVEN
,case when rk.afb=1 or rk.afb_znd_boekje=1 then dat_afb else 0 end as DAT_OPHEFFEN
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
,Case when aq.kred_limiet <> 0 then aq.kred_limiet else rk.kred_limiet end as inp_kred_limiet
,pi2.deb_perc
,case when inp_kred_limiet < pi.dlk_kred_grens_1 then pi.dlk_deb_perc_1
      when inp_kred_limiet < pi.dlk_kred_grens_2 then pi.dlk_deb_perc_2
      when inp_kred_limiet < pi.dlk_kred_grens_3 then pi.dlk_deb_perc_3
      when inp_kred_limiet < pi.dlk_kred_grens_4 then pi.dlk_deb_perc_4
      when inp_kred_limiet < pi.dlk_kred_grens_5 then pi.dlk_deb_perc_5
      else null 
 end as Maandperc_huidige_RSC_bij_afsluiten_contract
,(Maandperc_huidige_RSC_bij_afsluiten_contract + pi2.deb_perc) as Incl_indiv_op_afslag
from edv_sas.brek rk
join edv_sas.bpro pr 
on pr.bpro_oid=rk.bpro_oid 
--and pr.aktueel=1 
and pr.dlk=1
--and pr.dat_afloop =99999999
and pr.dat_effektief <= rk.dat_opening -- Eerste BPRO record
and pr.dat_afloop>rk.dat_opening
and rk.dwh_eind_dt is null
and pr.dwh_eind_dt is null
and pr.geldig_ind = 1
join edv_sas.bpre_type5 			pi 
on pi.bank_nr=rk.bank_nr 
and pi.bpro_oid=rk.bpro_oid 
and pi.pre_grp=5 
and pi.dat_effektief <= rk.dat_opening 
and pi.dat_afloop>rk.dat_opening
and pi.dwh_eind_dt is null
left outer join edv_sas.brcag_type1 aq
on aq.brek_oid=rk.brek_oid 
and aq.rcag_grp=1 
and aq.dat_effektief=rk.dat_opening 
and aq.dat_afloop>rk.dat_opening
and aq.dwh_eind_dt is null
and aq.geldig_ind = 1
left outer join edv_sas.bpre_type1 pi2 
on pi2.brek_oid=rk.brek_oid 
--and pi2.rek_nr=rk.rek_nr
and pi2.pre_grp=1 
and pi2.dat_effektief=rk.dat_opening 
and pi2.dat_afloop>pi2.dat_effektief
and pi2.dwh_eind_dt is null
and pi2.geldig_ind = 1
--where rk.rek_nr = 855455888
join
(select distinct rk1.brek_oid as brek_oid
from edv_sas.brek rk1
join edv_sas.bpro pr1 
on pr1.bpro_oid=rk1.bpro_oid 
and pr1.aktueel=1 
and pr1.dlk=1
and pr1.dat_afloop =99999999
and rk1.dwh_eind_dt is null
and pr1.dwh_eind_dt is null
and pr1.geldig_ind = 1
and pr1.rek_srt in (610,615,625,692)
)bb
on rk.brek_oid = bb.brek_oid
order by rk.bank_nr,rk.rek_srt,rk.rek_nr;
 

