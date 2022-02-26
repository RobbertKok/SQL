select
 coalesce(rco.contract_oid,sas.contract_oid) as contract_oid
,coalesce(rco.peil_dt,sas.peil_dt) as peil_dt
,sas.rente_bg
,sas.rente_bg_ref
,sas.rente_bg - sas.rente_bg_ref as sas_compensatie_mnd_bg
,((((sum(sas_compensatie_mnd_bg) OVER (partition by rco.contract_oid order by rco.peil_dt rows unbounded preceding )) - sas_compensatie_mnd_bg) * delta)/12)*-1 as sas_compensatie_ropr_mnd_bg
,(sas_compensatie_mnd_bg + sas_compensatie_ropr_mnd_bg) (decimal(18,6)) as sas_compensatie_totaal_bg

,rco.gem_maand_saldo_bg
,rco.SALDO_debetrente_bg
,
,rco.klant_rte_jaar_perc
,rco.marge as marge_bij_opening
,rco.referentie_rte_jaar_perc
,rco.ref_klant_rte_verschil
,rco.delta
,rco.compensatie_mnd_bg
,rco.compensatie_ropr_mnd_bg 
,(compensatie_mnd_bg + compensatie_ropr_mnd_bg) (decimal(18,6))  as compensatie_totaal_bg
,vzn.compensatie_bg as compensatie_voorziening_bg
from
s_dm_cdo.rekening_compensatie_gem_saldo rco
left join 
		(select contract_oid
		,laatste_dag_vd_maand as peil_dt
		,sum(aantal_dagen_tot_verval_rte_dag) aantal_dagen_tot_verval_rte_dag
		,avg(klant_rte_jaar_perc) klant_rte_jaar_perc
		,avg(referentie_rte_incl_marge_jaar_perc) referentie_rte_incl_marge_jaar_perc
		,sum(rente_pot_start) as rte_pot_start
		,sum(rente_pot_mutatie) as rte_pot_mut
		,sum(rente_pot_start + rente_pot_mutatie) as rente_bg
        ,sum(rente_pot_start_ref) as rte_pot_start_ref
		,sum(rente_pot_mutatie_ref) as rte_pot_mut_ref
		,sum(rente_pot_start_ref + rente_pot_mutatie_ref) as rente_bg_ref

		from s_dm_cdo.rentepot_mutaties
		where contract_oid in (
		201174960
		,259192653
		,198461483
		,342067388
		,206318120
		,266629516
		,244813962
		,277615125
		,241233934
		,211801888
		)
		group by 1,2
		)sas

on sas.contract_oid = rco.contract_oid
and sas. peil_dt = rco.peil_dt
left join S_DM_VariabeleRente.rente_berekening_agg vzn
on rco.contract_oid = vzn.contract_oid
order by 1,2;