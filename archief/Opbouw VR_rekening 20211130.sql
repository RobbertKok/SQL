drop table s_dm_cdo.vr_rekening;


create table s_dm_cdo.vr_rekening
as
(
select 
base.contract_oid
,rek_nr
--,klant_nr as klant_nr_brek
,cpl.partij_oid
,base.bank_nr
,rek_srt
,rek_srt_ingangs_dt  -- voorlopig uit actuele bpro
,bpro_oid   -- sleutel naar bpro voor latere reksrt veranderingen onderzoek
,zakelijk_prod_ind
,1 as product_nr
,rek_soort_naam
,opening_dt
,opgeheven_dt
,status_opgeheven_ind
,aanvangs_rte_maand_perc
,kred_limiet
,cast('edv_sas' as varchar(50)) as bronsysteem
from
(
 select 
 rk.brek_oid as contract_oid
,rk.klant_nr
,rk.bank_nr
,rk.rek_srt 
,cast(pr.dat_effektief -19000000 as date) as rek_srt_ingangs_dt
,pr.prod_naam as rek_soort_naam
,pr.zakelijk as zakelijk_prod_ind
,rk.rek_nr
,rk.bpro_oid
,rk.kred_limiet
,rk.saldo
,cast(rk.dat_opening -19000000 as date) as opening_dt
,case when rk.afb=1 or rk.afb_znd_boekje=1 then 'j' else 'n' end as status_opgeheven_ind
,cast((case when rk.afb=1 or rk.afb_znd_boekje=1 then dat_afb else null end)-19000000 as date)  as opgeheven_dt
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
,case when zeroifnull(aq.kred_limiet) <> 0 then aq.kred_limiet else zeroifnull(rk.kred_limiet) end as inp_kred_limiet
,pi2.deb_perc
,pi.deb_rte_per as rente_afreken_periode
,pi.deb_rte_dgn as rente_berekening_methode
,case when inp_kred_limiet < pi.dlk_kred_grens_1 then pi.dlk_deb_perc_1
      when inp_kred_limiet < pi.dlk_kred_grens_2 then pi.dlk_deb_perc_2
      when inp_kred_limiet < pi.dlk_kred_grens_3 then pi.dlk_deb_perc_3
      when inp_kred_limiet < pi.dlk_kred_grens_4 then pi.dlk_deb_perc_4
      when inp_kred_limiet < pi.dlk_kred_grens_5 then pi.dlk_deb_perc_5
      else null 
 end as perc_huidige_rsc_bij_afsluiten_contract
,case when pi.deb_rte_per = 9 then cast((perc_huidige_rsc_bij_afsluiten_contract/12) as decimal(4,2))    -- jaarperiode omzetten naar maand
      else perc_huidige_rsc_bij_afsluiten_contract
 end as maandperc_huidige_rsc_bij_afsluiten_contract
,(maandperc_huidige_rsc_bij_afsluiten_contract +  case when pi2.deb_rte_per = 9 
                                                   then cast((zeroifnull(pi2.deb_perc)/12) as decimal(4,2))
											      else  zeroifnull(pi2.deb_perc) 
    end) as aanvangs_rte_maand_perc
from edv_sas.brek rk
join edv_sas.bpro pr 
on pr.bpro_oid=rk.bpro_oid 
and pr.aktueel=1 
and pr.dlk=1
and pr.dat_afloop =99999999
and rk.dwh_eind_dt is null
and pr.dwh_eind_dt is null
and pr.geldig_ind = 1
left join edv_sas.bpre_type5 			pi 
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
where rk.rek_srt in (597,598,599,603,606,607,608,609,615,623,624,625,638,692,695,697,824,831,839,840,866,972,980,982)
)base
join dwh.contract_partij_l cpl
on base.contract_oid = cpl.contract_oid
and cpl.eind_dt = '9999-12-31'
--and cpl.geldig_ind = 1
left join dm_ster.dim_klant dk
on cpl.partij_oid = dk.partij_oid
and dk.dm_eind_dt = '9999-12-31'

and contract_partij_rol_cd in (1)
qualify row_number() over (partition by cpl.contract_oid order by relatie_eind_dt desc, begin_dt desc, geldig_ind desc, cpl.partij_oid desc,coalesce(dk.klant_uitstroom_dt,date'9999-12-31' ) desc) = 1

)with data unique primary index(contract_oid);