/* 25-1-2022 RK: toevoegen van persona Anna 
Het contract is gestart na 2006
• Het contract loopt nu nog
• Het contract is niet locked-up, geen opnameblokkade
• Het contract heeft niet in Bijzonder Beheer gezeten
• Het contract heeft een ongewijzigde kredietlimiet
• Het contract heeft geen omgetypeerde rekening
• Het contract is een SNS Zelfkrediet, RSC 572 bestaat niet 972?????
• Het contract heeft 1 rekeninghouder, contract gekoppeld aan 1 persoon gedurende looptijd
*/



drop table s_dm_variabelerente.rekening_klant_segment 

drop table s_dm_cdo.rekening_klant_segment;

create table s_dm_cdo.rekening_klant_segment
as (
select distinct 
 rek.contract_oid
,cpl.partij_oid as partij_rekhouder_oid
,'ANNA' as persona
,rek.rek_srt
,rek.rek_soort_naam
,rek.opgeheven_dt
,rek.opening_dt
,blk.blokkeringsreden_cd as blokkade_lockup
,blk1.blokkeringsreden_cd as blokkade_bijzonder_beheer
,case when ks.contract_oid > 0 or ks1.contract_oid > 0 then 1 else 0 end as kredietlimiet_wisseling_ind
,case when trx.contract_oid > 0 then 1 else 0 end as rek_soort_wisseling_ind
,case when par.contract_oid > 0 then 1 else 0 end as contract_met_1_klant_ind
from s_dm_variabelerente.rekening rek
left join dwh.contract_partij_l cpl
on rek.contract_oid = cpl.contract_oid
and cpl.eind_dt = '9999-12-31' and cpl.geldig_ind = 1
and cpl.relatie_eind_dt is null
and cpl.contract_partij_rol_cd = 1
left join dwh.sas_rekening_blokkering blk  --lockup op actueel moment
on blk.contract_oid= rek.contract_oid
and blk.eind_dt = '9999-12-31' and blk.geldig_ind = 1
and blk.blokkeringsreden_cd = 23 --- navraag bij Patrick welke blokkades dit moeten zijn

left join dwh.sas_rekening_blokkering blk1 -- geen bijzonderbeheer gedurende looptijd 
on blk1.contract_oid= rek.contract_oid
and blk1.blokkeringsreden_cd = 26 and blk1.geldig_ind = 1 

left join (select ks.contract_oid,ks.kredietlimiet_bg   -- contracten met wisseling van de kredietlimiet 
			     ,sum(kredietlimiet_bg) over (partition by ks.contract_oid order by ks.begin_dt rows between 1 preceding and 1 preceding) as vorige_record		
           from dwh.kredietlimiet_s ks
           where ks.limietsoort_cd = 30
           qualify sum(ks.kredietlimiet_bg) over (partition by ks.contract_oid order by ks.begin_dt rows between 1 preceding and 1 preceding)<> ks.kredietlimiet_bg
           )ks
on rek.contract_oid = ks.contract_oid

left join (select aq.brek_oid as contract_oid,aq.kred_limiet -- contracten met wisseling van de individuele kredietlimiet
			     ,sum(aq.kred_limiet) over (partition by aq.brek_oid order by dwh_begin_dt rows between 1 preceding and 1 preceding) as vorige_record
           from   edv_sas.brcag_type1 aq
           where  aq.rcag_grp=1 
            and  aq.brek_oid > 0 
           qualify sum(aq.kred_limiet) over (partition by aq.brek_oid order by aq.dwh_begin_dt rows between 1 preceding and 1 preceding)<> aq.kred_limiet
          )ks1
on rek.contract_oid = ks1.contract_oid          
left join  (select t.contract_oid,t.rekeningsoort_nr --- contracten met wisseling van rekeningsoort
                  ,sum(t.rekeningsoort_nr) over (partition by t.contract_oid order by t.peil_dt rows between 1 preceding and 1 preceding) as vorige_record
			from  S_DM_VariabeleRente.rekening_transactie t
			qualify sum(t.rekeningsoort_nr) over (partition by t.contract_oid order by t.peil_dt rows between 1 preceding and 1 preceding)<> t.rekeningsoort_nr
			)trx
on rek.contract_oid = trx.contract_oid  			
left join (select cpl.contract_oid						-- contracten met maar 1 gekoppelde partij op gedurende looptijd
				 ,count(distinct cpl.partij_oid) as aantal_partijen 
		   from dwh.contract_partij_l cpl
		   where 1=1
		   --and eind_dt = '9999-12-31' and geldig_ind = 1
		   --and relatie_eind_dt is null
		   and contract_partij_rol_cd in (1,2,3,4,5,10,11)
	       group by 1
		   having aantal_partijen = 1
		  )par
on rek.contract_oid = par.contract_oid  		  
where 1=1 
and rek.rek_srt = 972
and rek.opgeheven_dt is null
and blk.contract_oid is null
and blk1.contract_oid is null
and rek.opening_dt >= '2006-12-19'
and ks.contract_oid is null
and ks1.contract_oid is null
and trx.contract_oid is null
and par.contract_oid > 0
)with data unique primary index(contract_oid);




/* ANNA met andere DK contracten voor 2006 selecteren om deze in de tweede stap te verwijderen*/

select r.contract_oid,rek_overig.contract_oid as te_deleten_contract_oid
from s_dm_cdo.rekening_klant_segment r
join dwh.contract_partij_l cpl
on r.partij_rekhouder_oid = cpl.partij_oid
and cpl.eind_dt = '9999-12-31' and cpl.geldig_ind = 1 and cpl.relatie_eind_dt is null
join (select contract_oid from   S_DM_VariabeleRente.rekening
      minus
      select contract_oid from s_dm_cdo.rekening_klant_segment
)rek_overig
on cpl.contract_oid = rek_overig.contract_oid;


/* DELETE van ANNA persona's met andere DK contracten */

delete from s_dm_cdo.rekening_klant_segment 
where contract_oid in
(
select rek_overig.contract_oid as te_deleten_contract_oid
from s_dm_cdo.rekening_klant_segment r
join dwh.contract_partij_l cpl
on r.partij_rekhouder_oid = cpl.partij_oid
and cpl.eind_dt = '9999-12-31' and cpl.geldig_ind = 1 and cpl.relatie_eind_dt is null
join (select contract_oid from   S_DM_VariabeleRente.rekening
      minus
      select contract_oid from s_dm_cdo.rekening_klant_segment
)rek_overig
on cpl.contract_oid = rek_overig.contract_oid);
