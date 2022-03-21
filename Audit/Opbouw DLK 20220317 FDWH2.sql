SELECT DISTINCT
ACC_DOORLOPEND_KREDIET.BANK_NR,
ACC_DOORLOPEND_KREDIET.REK_NR,
ACC_DOORLOPEND_KREDIET.REK_SRT,
TRIM(ITEM_NAAM) AS REK_SRT_NAAM,
ACC_DOORLOPEND_KREDIET.DAT_OPENING_DT,
TRIM(ACC_DOORLOPEND_KREDIET.ACTUELE_STATUS_CD) AS STATUS,
ACC_DOORLOPEND_KREDIET.DAT_LTST_MUT_DT,
ACC_DOORLOPEND_KREDIET.OORSPRONKELIJK_KREDIET,
ACC_DOORLOPEND_KREDIET.KRED_LIMIET,
ABS(ACC_DOORLOPEND_KREDIET.SALDO) SALDO,
B.REDEN AS BB_REDEN,
TRIM(VRC.PARENT_WAARDE) BB_REDEN_NAAM,
CASE WHEN COUNT(R.KLANT_NR_VICE) OVER (PARTITION BY ACC_DOORLOPEND_KREDIET.REK_NR ORDER BY ACC_DOORLOPEND_KREDIET.REK_NR) > 1 THEN 'N' ELSE 'J' END AS EEN_REK_HDR,
CASE WHEN REK_SRT_DIFF <> 0 THEN 'J' ELSE 'N' END REK_SRT_DIFF,
CASE WHEN KRED_LIM_DIFF <> 0 THEN 'J' ELSE 'N' END KRED_LIM_DIFF,
-----------------------------------------------------------------------
-- kopie van Robert (rente percentages)
case when zeroifnull(ACC_DOORLOPEND_KREDIET.KRED_LIMIET) <> 0 then ACC_DOORLOPEND_KREDIET.KRED_LIMIET 
      when zeroifnull(rk.kred_limiet) <> 0 then zeroifnull(rk.kred_limiet)
      else ACC_DOORLOPEND_KREDIET.OORSPRONKELIJK_KREDIET
        end as inp_kred_limiet,
case when inp_kred_limiet < pi.dlk_kred_grens_1 then pi.dlk_deb_perc_1
      when inp_kred_limiet < pi.dlk_kred_grens_2 then pi.dlk_deb_perc_2
      when inp_kred_limiet < pi.dlk_kred_grens_3 then pi.dlk_deb_perc_3
      when inp_kred_limiet < pi.dlk_kred_grens_4 then pi.dlk_deb_perc_4
      when inp_kred_limiet < pi.dlk_kred_grens_5 then pi.dlk_deb_perc_5
      else null 
 end as perc_huidige_rsc_bij_afsluiten_contract,
case when pi.deb_rte_per = 9 then cast((perc_huidige_rsc_bij_afsluiten_contract/12) as decimal(4,2))    -- jaarperiode omzetten naar maand
      else perc_huidige_rsc_bij_afsluiten_contract
end as maandperc_huidige_rsc_bij_afsluiten_contract,
(maandperc_huidige_rsc_bij_afsluiten_contract +  case when pi2.deb_rte_per = 9 
                                                   then cast((zeroifnull(pi2.deb_perc)/12) as decimal(4,2))
                                                                             else  zeroifnull(pi2.deb_perc) 
    end) as aanvangs_rte_maand_perc
-----------------------------------------------------------------------
FROM FDWH2_SAL_SAS.ACC_DOORLOPEND_KREDIET
LEFT JOIN FDWH2_SAL_SAS.ACC_BKOP_2 R
  ON R.EWBK_BREK_ID =  ACC_DOORLOPEND_KREDIET.EWBK_BREK_ID
AND R.SNAPSHOT_ID = ACC_DOORLOPEND_KREDIET.SNAPSHOT_ID
AND R.KOPP_SRT IN (1,2,3,10,110)
AND R.AKTUEEL = 1
JOIN (
SELECT EWBK_BREK_ID, MIN(REK_SRT)-MAX(REK_SRT) REK_SRT_DIFF, MIN(KRED_LIMIET)-MAX(KRED_LIMIET) KRED_LIM_DIFF
FROM FDWH2_SAL_SAS.ACC_DOORLOPEND_KREDIET GROUP BY 1
) AS B_DIFF ON B_DIFF.EWBK_BREK_ID = ACC_DOORLOPEND_KREDIET.EWBK_BREK_ID
LEFT JOIN FDWH2_SAL_SAS.SRC_REKENING_BLOKKADE_BRCAG_BLOKK B 
       ON B.EWBK_BREK_ID = ACC_DOORLOPEND_KREDIET.EWBK_BREK_ID
      AND B.REK_NR = ACC_DOORLOPEND_KREDIET.REK_NR
      AND B.SNAPSHOT_ID = ACC_DOORLOPEND_KREDIET.SNAPSHOT_ID
      AND B.REDEN IN (26,61,226)
LEFT JOIN (
SELECT CHILD_CD, SNAPSHOT_ID, PARENT_WAARDE
FROM FDWH2_SAL_EBX.ACC_REF_RELATIE_CODERING_VIEW AS VRC
WHERE VRC.PARENT_BRON = 'BICC'  
  AND VRC.CHILD_BRON = 'SAS'  
  AND lower ( VRC.RELATIENAAM ) = 'blokkeringsreden reden'  
  AND VRC.PARENT_CONTEXT_NAAM = 'BICC'  
  AND VRC.CHILD_CONTEXT_NAAM = 'SAS'
) VRC ON 
VRC.CHILD_CD = B.REDEN
AND VRC.SNAPSHOT_ID = B.SNAPSHOT_ID
INNER JOIN FDWH2_SAL_EBX.ACC_REF_PRODUCTSTRUCTUUR_VIEW AS EBX_PRODUCTSTRUCTUUR
   ON EBX_PRODUCTSTRUCTUUR.ITEM_NR = ACC_DOORLOPEND_KREDIET.REK_SRT
  AND EBX_PRODUCTSTRUCTUUR.BANK_NR = ACC_DOORLOPEND_KREDIET.BANK_NR
  AND EBX_PRODUCTSTRUCTUUR.SNAPSHOT_ID = ACC_DOORLOPEND_KREDIET.SNAPSHOT_ID
  AND EBX_PRODUCTSTRUCTUUR.BRON = 'SAS' 
  AND EBX_PRODUCTSTRUCTUUR.ITEMCATEGORIE_NR = '1'
  AND EBX_PRODUCTSTRUCTUUR.CONTRACTSOORT_NR = '1' 
  AND EBX_PRODUCTSTRUCTUUR.HOOFDPRODUCTGROEP_NR = '5'
  AND EBX_PRODUCTSTRUCTUUR.ZAKELIJK_IND = 0
-----------------------------------------------------------------------
-- kopie van Robert (rente ophalen)
JOIN FDWH2_SAL_SAS.ACC_BREK rk
ON rk.EWBK_BREK_ID = ACC_DOORLOPEND_KREDIET.EWBK_BREK_ID
AND rk.SNAPSHOT_ID = ACC_DOORLOPEND_KREDIET.SNAPSHOT_ID
--left 
left join edv_sas.bpre_type1 pi2 
on  pi2.rek_nr=CAST(rk.rek_nr AS BIGINT)
and pi2.pre_grp=1 
and pi2.dat_effektief=20061219 
and pi2.dat_afloop>20061219
and pi2.dwh_begin_dt <= date'2006-12-19'
and (pi2.dwh_eind_dt > date'2006-12-19' or pi2.dwh_eind_dt is null)
and pi2.geldig_ind = 1
left join edv_sas.bpre_type5                 pi 
on pi.bank_nr=CAST(rk.bank_nr AS BIGINT) 
and pi.rek_srt=CAST(rk.rek_srt AS BIGINT) 
and pi.pre_grp=5 
and pi.dat_effektief <= CASE WHEN rk.rek_srt <> 980 THEN 20061219 ELSE 20070517 END
and pi.dat_afloop> CASE WHEN rk.rek_srt <> 980 THEN 20061219 ELSE 20070517 END
and pi.dwh_begin_dt <= CASE WHEN rk.rek_srt <> 980 THEN date'2006-12-19' ELSE  date'2007-05-17' END
and (pi.dwh_eind_dt > CASE WHEN rk.rek_srt <> 980 THEN date'2006-12-19' ELSE  date'2007-05-17' END or pi.dwh_eind_dt is null)
and pi.geldig_ind = 1
-----------------------------------------------------------------------
WHERE ACC_DOORLOPEND_KREDIET.DAT_OPENING_DT >= '2006-12-19'
AND (SELECT MAX(SNAPSHOT_ID) FROM FDWH2_SAL_SAS.ACC_DOORLOPEND_KREDIET) = ACC_DOORLOPEND_KREDIET.SNAPSHOT_ID
;
