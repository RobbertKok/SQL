

drop table s_dm_variabelerente.rekening;

create table s_dm_variabelerente.rekening
as
(
select 
base.contract_oid
,rek_srt
,inp_kred_limiet
,opening_dt
,opgeheven_dt
,ref.maand
,cast(aanvangs_rte_maand_perc *12  as decimal(14,2)) as aanvangs_rte_jaar_perc
,aanvangs_rte_jaar_perc- referentie_jaar_rente_perc  as marge
,cast(ref.referentie_rente*100 as decimal(14,2)) as referentie_jaar_rente_perc
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
,zeroifnull(rk.kred_limiet)		as kred_limiet_brek
,zeroifnull(ks.kredietlimiet_bg) 	as kred_limiet_sat
,zeroifnull(aq.kred_limiet)		as kred_limiet_indiv
,ks.limietsoort_cd
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
,case when zeroifnull(aq.kred_limiet) <> 0 then aq.kred_limiet 
      when zeroifnull(rk.kred_limiet) <> 0 then zeroifnull(rk.kred_limiet)
      else zeroifnull(ks.kredietlimiet_bg)
	  end as inp_kred_limiet
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

left join dwh.kredietlimiet_s ks
on rk.brek_oid = ks.contract_oid
and kredietlimiet_start_dt <= cast(rk.dat_opening -19000000 as date)
and kredietlimiet_eind_dt > cast(rk.dat_opening -19000000 as date)
and begin_dt <= cast(rk.dat_opening -19000000 as date)
and eind_dt > cast(rk.dat_opening -19000000 as date)
--and ks.limietsoort_cd = 30

left join edv_sas.bpre_type5 			pi 
on pi.bank_nr=rk.bank_nr 
and pi.bpro_oid=rk.bpro_oid 
and pi.pre_grp=5 
and pi.dat_effektief <= rk.dat_opening 
and pi.dat_afloop>rk.dat_opening
and pi.dwh_begin_dt <= cast(rk.dat_opening -19000000 as date)
and (pi.dwh_eind_dt > cast(rk.dat_opening -19000000 as date) or pi.dwh_eind_dt is null)
and pi.geldig_ind = 1
left outer join edv_sas.brcag_type1 aq
on aq.brek_oid=rk.brek_oid 
and aq.rcag_grp=1 
and aq.dat_effektief=rk.dat_opening 
and aq.dat_afloop>rk.dat_opening
and aq.dwh_begin_dt <= cast(rk.dat_opening -19000000 as date)
and (aq.dwh_eind_dt > cast(rk.dat_opening -19000000 as date) or aq.dwh_eind_dt is null)
and aq.geldig_ind = 1
left outer join edv_sas.bpre_type1 pi2 
on pi2.brek_oid=rk.brek_oid 
--and pi2.rek_nr=rk.rek_nr
and pi2.pre_grp=1 
and pi2.dat_effektief=rk.dat_opening 
and pi2.dat_afloop>pi2.dat_effektief
and pi2.dwh_begin_dt <= cast(rk.dat_opening -19000000 as date)
and (pi2.dwh_eind_dt > cast(rk.dat_opening -19000000 as date) or pi2.dwh_eind_dt is null)
and pi2.geldig_ind = 1
where rk.rek_srt in (597,598,599,603,606,607,608,609,615,623,624,625,638,692,695,697,824,831,839,840,866,972,980,982)
and rk.dat_opening >= 20061219                                                              -- Vanaf 2006-12-19 zijn de eerste keer de rentetabellen ontsloten
)base
join s_dm_variabelerente.referentie_rente ref
on (extract(year from opening_dt)*100 + extract(month from opening_dt)) = (extract(year from ref.maand)*100 + extract(month from ref.maand))
and ref.product_nr = 1
WHERE opening_dt >= (SELECT MIN(DWH_BEGIN_DT) from edv_sas.bpre_type5) 
)with data unique primary index(contract_oid);



insert into s_dm_variabelerente.rekening
select 
 t1.contract_oid
,rek_srt
,inp_kred_limiet
,opening_dt
,opgeheven_dt
,ref.maand
,cast(aanvangs_rte_maand_perc *12  as decimal(14,2)) as aanvangs_rte_jaar_perc
,aanvangs_rte_jaar_perc- referentie_jaar_rente_perc  as marge
,cast(ref.referentie_rente*100 as decimal(14,2)) as referentie_jaar_rente_perc
from
(select 
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
,kred_limiet_brek
,kred_limiet_sat
,kred_limiet_indiv
,limietsoort_cd
,inp_kred_limiet
,aanvangs_rte_maand_perc
,rente_afreken_periode
,rente_berekening_methode
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
,zeroifnull(rk.kred_limiet)		as kred_limiet_brek
,zeroifnull(ks.kredietlimiet_bg) 	as kred_limiet_sat
,zeroifnull(aq.kred_limiet)		as kred_limiet_indiv
,ks.limietsoort_cd
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
,case when zeroifnull(aq.kred_limiet) <> 0 then aq.kred_limiet 
      when zeroifnull(rk.kred_limiet) <> 0 then zeroifnull(rk.kred_limiet)
      else zeroifnull(ks.kredietlimiet_bg)
	  end as inp_kred_limiet
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
left join dwh.kredietlimiet_s ks
on rk.brek_oid = ks.contract_oid
and kredietlimiet_start_dt <= DATE'2006-12-19'
and kredietlimiet_eind_dt > DATE'2006-12-19'
and begin_dt <= DATE'2006-12-19'
and eind_dt > DATE'2006-12-19'
--and ks.limietsoort_cd = 30

left join edv_sas.bpre_type5 			pi 
on pi.bank_nr=rk.bank_nr 
and pi.bpro_oid=rk.bpro_oid 
and pi.pre_grp=5 
and pi.dat_effektief <= CASE WHEN rk.rek_srt <> 980 THEN 20061219 ELSE 20070517 END
and pi.dat_afloop> CASE WHEN rk.rek_srt <> 980 THEN 20061219 ELSE 20070517 END
and pi.dwh_begin_dt <= CASE WHEN rk.rek_srt <> 980 THEN date'2006-12-19' ELSE  date'2007-05-17' END
and (pi.dwh_eind_dt > CASE WHEN rk.rek_srt <> 980 THEN date'2006-12-19' ELSE  date'2007-05-17' END or pi.dwh_eind_dt is null)
and pi.geldig_ind = 1
left outer join edv_sas.brcag_type1 aq
on aq.brek_oid=rk.brek_oid 
and aq.rcag_grp=1 
and aq.dat_effektief=20061219 
and aq.dat_afloop>20061219
and aq.dwh_begin_dt <= date'2006-12-19'
and (aq.dwh_eind_dt > date'2006-12-19' or aq.dwh_eind_dt is null)
and aq.geldig_ind = 1
left outer join edv_sas.bpre_type1 pi2 
on pi2.brek_oid=rk.brek_oid 
--and pi2.rek_nr=rk.rek_nr
and pi2.pre_grp=1 
and pi2.dat_effektief=20061219 
and pi2.dat_afloop>20061219
and pi2.dwh_begin_dt <= date'2006-12-19'
and (pi2.dwh_eind_dt > date'2006-12-19' or pi2.dwh_eind_dt is null)
and pi2.geldig_ind = 1
where rk.rek_srt in (597,598,599,603,606,607,608,609,623,624,638,695,697,824,831,839,840,866,972,980,982)
and rk.dat_opening < 20061219                                                              -- Vanaf 2006-12-19 zijn de eerste keer de rentetabellen ontsloten
)base
)T1
left join s_dm_variabelerente.referentie_rente ref
on (extract(year from t1.opening_dt)*100 + extract(month from t1.opening_dt)) = (extract(year from ref.maand)*100 + extract(month from ref.maand))
and ref.product_nr = 1

;