-- /* rentestanden per bank, rekeningsoort op dagbasis */
-- 
drop table s_dm_variabelerente.rte_dag_staffel;

create table s_dm_variabelerente.rte_dag_staffel
as(
select
 bnk_nr as bank_nr
,rek_srt
,dim_datum_id as peil_dag_dt
,pi.deb_rte_per
,d1.decode_omschr as rente_afreken_periode
,pi.deb_rte_dgn
,d2.decode_omschr as rente_berekening_methode
,pi.dat_effektief
,pi.dat_afloop
,pi.dwh_begin_dt
,pi.dwh_eind_dt
--,sum(pi.dlk_deb_perc_1 ) over (partition by bank_nr,rek_srt order by peil_dt rows between 1 preceding and 1 preceding) as vorige_record
,case when pi.deb_rte_per = 6 then  (pi.dlk_deb_perc_1*12 (decimal(14,2))) else (pi.dlk_deb_perc_1 (decimal(14,2))) end as dlk_deb_perc_1_jr
,pi.dlk_kred_grens_1
,case when pi.deb_rte_per = 6 then  (pi.dlk_deb_perc_2*12 (decimal(14,2))) else (pi.dlk_deb_perc_2 (decimal(14,2))) end as dlk_deb_perc_2_jr
,pi.dlk_kred_grens_2
,case when pi.deb_rte_per = 6 then  (pi.dlk_deb_perc_3*12 (decimal(14,2))) else (pi.dlk_deb_perc_3 (decimal(14,2))) end as dlk_deb_perc_3_jr
,pi.dlk_kred_grens_3
,case when pi.deb_rte_per = 6 then  (pi.dlk_deb_perc_4*12 (decimal(14,2))) else (pi.dlk_deb_perc_4 (decimal(14,2))) end as dlk_deb_perc_4_jr
,pi.dlk_kred_grens_4
,case when pi.deb_rte_per = 6 then  (pi.dlk_deb_perc_5*12 (decimal(14,2))) else (pi.dlk_deb_perc_5 (decimal(14,2))) end as dlk_deb_perc_5_jr
,pi.dlk_kred_grens_5

from edv_sas.bpre_type5 			pi 
join dm_ster.dim_datum dd
on pi.pre_grp=5
and pi.geldig_ind = 1
and pi.dat_effektief <= dd.datum_jjjjmmdd 
and pi.dat_afloop  > dd.datum_jjjjmmdd 
and pi.dwh_begin_dt <= dd.dim_datum_id
and (pi.dwh_eind_dt > dd.dim_datum_id or pi.dwh_eind_dt is null)
--where dd.ultimo_maand_ind = 1
left join dwh.sas_decode d1
on pi.deb_rte_per_oid = d1.decode_oid
and d1.eind_dt = '9999-12-31'  and d1.geldig_ind = 1
left join dwh.sas_decode d2
on pi.deb_rte_dgn_oid = d2.decode_oid
and d2.eind_dt = '9999-12-31'  and d2.geldig_ind = 1
where dim_datum_id between date'2006-12-19' and date
and rek_srt in (597,598,599,603,606,607,608,609,615,623,624,625,638,692,695,697,824,831,839,840,866,972,980,982)

)with data unique primary index(bank_nr,rek_srt,peil_dag_dt);

-- insert rentes voor 2006
insert into s_dm_variabelerente.rte_dag_staffel
select
 bnk_nr as bank_nr
,rek_srt
,dim_datum_id as peil_dag_dt
,pi.deb_rte_per
,d1.decode_omschr as rente_afreken_periode
,pi.deb_rte_dgn
,d2.decode_omschr as rente_berekening_methode
,pi.dat_effektief
,pi.dat_afloop
,pi.dwh_begin_dt
,pi.dwh_eind_dt
--,sum(pi.dlk_deb_perc_1 ) over (partition by bank_nr,rek_srt order by peil_dt rows between 1 preceding and 1 preceding) as vorige_record
,case when pi.deb_rte_per = 6 then  (pi.dlk_deb_perc_1*12 (decimal(14,2))) else (pi.dlk_deb_perc_1 (decimal(14,2))) end as dlk_deb_perc_1_jr
,pi.dlk_kred_grens_1
,case when pi.deb_rte_per = 6 then  (pi.dlk_deb_perc_2*12 (decimal(14,2))) else (pi.dlk_deb_perc_2 (decimal(14,2))) end as dlk_deb_perc_2_jr
,pi.dlk_kred_grens_2
,case when pi.deb_rte_per = 6 then  (pi.dlk_deb_perc_3*12 (decimal(14,2))) else (pi.dlk_deb_perc_3 (decimal(14,2))) end as dlk_deb_perc_3_jr
,pi.dlk_kred_grens_3
,case when pi.deb_rte_per = 6 then  (pi.dlk_deb_perc_4*12 (decimal(14,2))) else (pi.dlk_deb_perc_4 (decimal(14,2))) end as dlk_deb_perc_4_jr
,pi.dlk_kred_grens_4
,case when pi.deb_rte_per = 6 then  (pi.dlk_deb_perc_5*12 (decimal(14,2))) else (pi.dlk_deb_perc_5 (decimal(14,2))) end as dlk_deb_perc_5_jr
,pi.dlk_kred_grens_5

from edv_sas.bpre_type5 			pi 
join dm_ster.dim_datum dd
on pi.pre_grp=5
and pi.geldig_ind = 1
and pi.dat_effektief <= dd.datum_jjjjmmdd 
and pi.dat_afloop  > dd.datum_jjjjmmdd 
and pi.dwh_begin_dt <= date'2006-12-19'
and (pi.dwh_eind_dt > date'2006-12-19' or pi.dwh_eind_dt is null)
--where dd.ultimo_maand_ind = 1
left join dwh.sas_decode d1
on pi.deb_rte_per_oid = d1.decode_oid
and d1.eind_dt = '9999-12-31'  and d1.geldig_ind = 1
left join dwh.sas_decode d2
on pi.deb_rte_dgn_oid = d2.decode_oid
and d2.eind_dt = '9999-12-31'  and d2.geldig_ind = 1
where dim_datum_id between date'1998-01-01' and date'2006-12-18'
and rek_srt in (597,598,599,603,606,607,608,609,615,623,624,625,638,692,695,697,824,831,839,840,866,972,980,982)
;


/* dagstanden tabel op contractniveau klantrente percentage, referentierente percentage en marge bij aanvang contract  */

drop table s_dm_variabelerente.rekening_dagbasis;

create table s_dm_variabelerente.rekening_dagbasis
as
(
select 
kal.dim_datum_id as peil_dag_dt
,kal.ultimo_maand_dt
,kal.dag_vd_maand
,rk.contract_oid
--,rk.bank_nr
,cast(pr.effectief_dt -19000000 as date) as rek_srt_ingangs_dt
,pr.productnaam_tkt as rek_soort_naam
,pr.rekeningsoort_nr
,rk.rek_nr
--,zeroifnull(rk.kredietlimiet_bg)		as kred_limiet_brek
,zeroifnull(ks.kredietlimiet_bg) 	as kred_limiet_sat
,zeroifnull(aq.kred_limiet)		as kred_limiet_indiv
,case when zeroifnull(aq.kred_limiet) <> 0 then aq.kred_limiet  -- Dagelijkse limiet opzoeken (andere methodiek ivm rente op dagbasis bepalen ipv alleen bij aanvang)
      else zeroifnull(ks.kredietlimiet_bg)
      end
      as inp_kred_limiet2
,sal.saldo_bg
,vr.opening_dt
,vr.opgeheven_dt
,pi.dlk_deb_perc_1_jr
,pi.dlk_kred_grens_1
,pi.dlk_deb_perc_2_jr
,pi.dlk_kred_grens_2
,pi.dlk_deb_perc_3_jr
,pi.dlk_kred_grens_3
,pi.dlk_deb_perc_4_jr
,pi.dlk_kred_grens_4
,pi.dlk_deb_perc_5_jr
,pi.dlk_kred_grens_5
,pi2.deb_rte_per as deb_rte_per_indiv
,zeroifnull(pi2.deb_perc) as rte_indiv_perc
,pi.deb_rte_per
,pi.rente_afreken_periode
,pi.deb_rte_dgn
,pi.rente_berekening_methode
,case when inp_kred_limiet2 < pi.dlk_kred_grens_1 then pi.dlk_deb_perc_1_jr
      when inp_kred_limiet2 < pi.dlk_kred_grens_2 then pi.dlk_deb_perc_2_jr
      when inp_kred_limiet2 < pi.dlk_kred_grens_3 then pi.dlk_deb_perc_3_jr
      when inp_kred_limiet2 < pi.dlk_kred_grens_4 then pi.dlk_deb_perc_4_jr
      when inp_kred_limiet2 < pi.dlk_kred_grens_5 then pi.dlk_deb_perc_5_jr
      else null 
 end 
 as rte_jaar_pct
 ,(rte_jaar_pct +  case when pi2.deb_rte_per = 9 
                        then cast((zeroifnull(rte_indiv_perc)) as decimal(14,4))
						else  cast(zeroifnull( rte_indiv_perc*12) as decimal(14,4))
                   end)
 as klant_rte_jaar_perc
,cast((ref.referentie_rente*100.000) as decimal(14,2)) as  referentie_rte_jaar_perc    
,vr.marge
,referentie_rte_jaar_perc + Marge as referentie_rte_incl_marge_jaar_perc
,case when sum(klant_rte_jaar_perc) over (partition by rk.contract_oid order by peil_dag_dt rows between 1 preceding and 1 preceding) <> klant_rte_jaar_perc then 1 else 0  end as rte_wissel
,case when sum(inp_kred_limiet2) over (partition by rk.contract_oid order by peil_dag_dt rows between 1 preceding and 1 preceding)  <> inp_kred_limiet2 then 1 else 0 end as lim_wissel

from (select dim_datum_id
,datum_jjjjmmdd
,laatste_dag_vd_maand as ultimo_maand_dt
,dag_vd_maand
from dm_ster.dim_datum
where kalender_datum >= '2006-12-19' and kalender_datum < date
)kal
join dwh.sas_rekening rk
on rk.begin_dt <= kal.dim_datum_id
and (rk.eind_dt > kal.dim_datum_id)
and rk.geldig_ind = 1

--join s_dm_variabelerente.rekening_klant_segment anna                      --ALLEEN de persona ANNA : per groep draaien
join s_dm_variabelerente.rekening vr
on rk.contract_oid = vr.contract_oid
join dwh.contract_product_l prd
on rk.contract_oid = prd.contract_oid
and  prd.begin_dt <= kal.dim_datum_id
and (prd.eind_dt > kal.dim_datum_id)
and prd.geldig_ind = 1
join DWH.sas_rekeningsoortkenmerk pr 
on pr.product_oid=prd.product_oid 
and pr.actueel_ind=1 
and pr.DOORLOPEND_KREDIET_IND =1
and pr.afloop_dt =99999999
and pr.eind_dt = '9999-12-31'
and pr.geldig_ind = 1

left join dwh.kredietlimiet_s ks
on rk.contract_oid = ks.contract_oid
and ks.kredietlimiet_start_dt <= kal.dim_datum_id
and (ks.kredietlimiet_eind_dt > kal.dim_datum_id or ks.kredietlimiet_eind_dt is null)
and ks.begin_dt <= kal.dim_datum_id
and ks.eind_dt > kal.dim_datum_id
and ks.limietsoort_cd = 30
left join dwh.contract_saldo_s sal
on rk.contract_oid = sal.contract_oid
and sal.saldo_start_dt <= kal.dim_datum_id
and (sal.saldo_eind_dt > kal.dim_datum_id or sal.saldo_eind_dt is null)
and sal.begin_dt <= kal.dim_datum_id
and sal.eind_dt > kal.dim_datum_id
and sal.geldig_ind = 1

left join s_dm_variabelerente.rte_dag_staffel	pi 
on pi.bank_nr=pr.bank_nr 
and pi.rek_srt=pr.rekeningsoort_nr 
and pi.peil_dag_dt = kal.dim_datum_id

left outer join edv_sas.brcag_type1 aq
on aq.brek_oid=rk.contract_oid 
and aq.rcag_grp=1 
and aq.dat_effektief<= kal.datum_jjjjmmdd
and aq.dat_afloop> kal.datum_jjjjmmdd
and aq.dwh_begin_dt <= kal.dim_datum_id
and (aq.dwh_eind_dt > kal.dim_datum_id or aq.dwh_eind_dt is null)
and aq.geldig_ind = 1
left outer join edv_sas.bpre_type1 pi2 
on pi2.brek_oid=rk.contract_oid 
and pi2.pre_grp=1 
and pi2.dat_effektief<= kal.datum_jjjjmmdd  
and pi2.dat_afloop> kal.datum_jjjjmmdd
and pi2.dwh_begin_dt <= kal.dim_datum_id
and (pi2.dwh_eind_dt > kal.dim_datum_id or pi2.dwh_eind_dt is null)
and pi2.geldig_ind = 1

left join S_DM_VariabeleRente.referentie_rente ref
on ref.maand = kal.ultimo_maand_dt
and ref.product_nr = 1

where pr.rekeningsoort_nr in (597,598,599,603,606,607,608,609,615,623,624,625,638,692,695,697,824,831,839,840,866,972,980,982)
and vr.opening_dt >= date'2006-12-19'                                                              -- Vanaf 2006-12-19 zijn de eerste keer de rentetabellen ontsloten
--and rk.contract_oid = 461191204     --TEST
)with data unique primary index(contract_oid,peil_dag_dt);


