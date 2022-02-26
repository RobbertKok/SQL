/* basis rekening tabel met alle actuele dk rekeningen */ 
drop table s_dm_cdo.vr_rekening_DWH;

create table s_dm_cdo.vr_rekening_DWH
as
(
select 
base.contract_oid
,rek_nr
--,klant_nr as klant_nr_brek
,cpl.partij_oid
,base.bank_nr
,rek_srt
--,rek_srt_ingangs_dt  -- voorlopig uit actuele bpro
--,bpro_oid   -- sleutel naar bpro voor latere reksrt veranderingen onderzoek
,zakelijk_prod_ind
,1 as product_nr
,rek_soort_naam
,opening_dt
,opgeheven_dt
,status_opgeheven_ind
,aanvangs_rte_maand_perc
,kredietlimiet_bg
,cast('edv_sas' as varchar(50)) as bronsysteem
from
(
 select 
 rk.contract_oid
,rk.klant_nr
,rk.bank_nr
,rk.rekeningsoort_nr as rek_srt
--,cast(pr.dat_effektief -19000000 as date) as rek_srt_ingangs_dt
,pr.productnaam_tkt as rek_soort_naam
,pr.zakelijk_ind as zakelijk_prod_ind
,rk.rek_nr
--,rk.bpro_oid
,rk.kredietlimiet_bg
--,rk.saldo
,cast(rk.opening_dt -19000000 as date) as opening_dt
,case when rk.afbetaald_ind=1 or rk.afbetaald_zonder_boekje_ind=1 then 'j' else 'n' end as status_opgeheven_ind
,cast((case when rk.afbetaald_ind=1 or rk.afbetaald_zonder_boekje_ind=1 then rk.datum_afbetaald_dt else null end)-19000000 as date)  as opgeheven_dt
,pi.DK_DEBETRENTE_1_PCT
,pi.DEBET_KREDIETGRENS_1_PCT
,pi.DK_DEBETRENTE_2_PCT
,pi.DEBET_KREDIETGRENS_2_PCT
,pi.DK_DEBETRENTE_3_PCT
,pi.DEBET_KREDIETGRENS_3_PCT
,pi.DK_DEBETRENTE_4_PCT
,pi.DEBET_KREDIETGRENS_4_PCT
,pi.DK_DEBETRENTE_5_PCT
,pi.DEBET_KREDIETGRENS_5_PCT
,case when aq.KREDIETLIMIET_BG <> 0 then aq.KREDIETLIMIET_BG else rk.KREDIETLIMIET_BG end as inp_kred_limiet
,pi2.DEBETRENTE_PCT
,pi.DEBETRENTE_PERIODE_CD as rente_afreken_periode
,pi.DEBETRENTE_DAGEN_CD as rente_berekening_methode
,case when inp_kred_limiet < pi.DEBET_KREDIETGRENS_1_PCT then pi.DK_DEBETRENTE_1_PCT
      when inp_kred_limiet < pi.DEBET_KREDIETGRENS_2_PCT then pi.DK_DEBETRENTE_2_PCT
      when inp_kred_limiet < pi.DEBET_KREDIETGRENS_3_PCT then pi.DK_DEBETRENTE_3_PCT
      when inp_kred_limiet < pi.DEBET_KREDIETGRENS_4_PCT then pi.DK_DEBETRENTE_4_PCT
      when inp_kred_limiet < pi.DEBET_KREDIETGRENS_5_PCT then pi.DK_DEBETRENTE_5_PCT
      else null 
 end as perc_huidige_rsc_bij_afsluiten_contract
,case when pi.DEBETRENTE_PERIODE_CD = 9 then cast((perc_huidige_rsc_bij_afsluiten_contract/12) as decimal(4,2))    -- jaarperiode omzetten naar maand
      else perc_huidige_rsc_bij_afsluiten_contract
 end as maandperc_huidige_rsc_bij_afsluiten_contract
,(maandperc_huidige_rsc_bij_afsluiten_contract + zeroifnull(pi2.DEBETRENTE_PCT)) as aanvangs_rte_maand_perc
from dwh.sas_rekening_lf rk
join dwh.contract_product_l cpl
on rk.contract_oid = cpl.contract_oid
and rk.eind_dt = '9999-12-31' and rk.geldig_ind = 1
and cpl.eind_dt = '9999-12-31' and cpl.geldig_ind = 1
join dwh.sas_rekeningsoortkenmerk pr 
on cpl.product_oid = pr.product_oid
--and pr.aktueel=1 
and pr.doorlopend_krediet_ind= 1
and pr.afloop_dt =99999999
and pr.eind_dt = '9999-12-31'
and pr.geldig_ind = 1
left join dwh.sas_dk_renteconditie  pi --edv_sas.bpre_type5 			
on pi.bank_nr=rk.bank_nr 
and pi.product_oid=cpl.product_oid 
and pi.progr_renteconditiegroep_cd=5 
and pi.effectief_dt <= rk.opening_dt
and pi.afloop_dt> rk.opening_dt
and pi.eind_dt = '9999-12-31' and pi.geldig_ind=1
left outer join dwh. sas_kredietlimiet_hist aq --edv_sas.brcag_type1 
on aq.contract_oid=rk.contract_oid 
and aq.CONTROLE_AGENDERINGGRP_CD=1 
and aq.effectief_dt=rk.opening_dt
and aq.afloop_dt>rk.opening_dt
and aq.eind_dt = '9999-12-31'
and aq.geldig_ind = 1
left outer join dwh.SAS_PROGRESSIEVE_RENTECONDITIE  pi2 --edv_sas.bpre_type1 pi2 
on pi2.contract_oid=rk.contract_oid 
--and pi2.rek_nr=rk.rek_nr
and pi2.PROGR_RENTECONDITIEGROEP_CD=1 
and pi2.effectief_dt=rk.opening_dt 
and pi2.afloop_dt>pi2.effectief_dt
and pi2.eind_dt = '9999-12-31'
and pi2.geldig_ind = 1
where rk.rekeningsoort_nr in (597,598,599,603,606,607,608,609,615,623,624,625,638,692,695,697,824,831,839,840,866,972,980,982)

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

)with data unique primary index(contract_oid)
;


