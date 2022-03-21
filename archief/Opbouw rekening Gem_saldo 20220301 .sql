
drop table s_dm_variabelerente.financiele_transacties;

create table s_dm_variabelerente.financiele_transacties
as(
sel
trx.bank_nr
,trx.rekeningsoort_nr
,trx.contract_oid
,cast(trx.journaal_dt-19000000 as date) as trx_journaal_dt
,cast(trx.valuta_dt-19000000 as date) as trx_valuta_dt
,trx.volg_nr
,(trx.transactiegroep_cd*1000 + trx.transactiesubgroep_cd*100+trx.transactiesoort_cd) as transactiesoort
,trx.transactie_bg
,trx.VORIG_CONTRACT_SALDO_BG
,ultimo_maand_dt
,dag_vd_maand
from dwh.sas_rekening_transactie trx 
join s_dm_variabelerente.rekening rek               --ALLEEN de persona ANNA : per groep draaien
on trx.contract_oid = rek.contract_oid
join (select dim_datum_id
,datum_jjjjmmdd
,laatste_dag_vd_maand as ultimo_maand_dt
,dag_vd_maand
from dm_ster.dim_datum
where kalender_datum >= '2006-12-19' and kalender_datum < date
)dd
on trx_journaal_dt = dd.dim_datum_id
--where contract_oid = 198461483    --TEST
)with data primary index(contract_oid)
;


/* Table met gemiddeld maandsaldo */

drop table s_dm_variabelerente.rekening_gem_saldo;

create table s_dm_variabelerente.rekening_gem_saldo
as
(
select contract_oid
,laatste_dag_vd_maand  as peil_dt
,sum(saldo_valuta_dt*dagen_saldo_tussen_maandstanden) as teller
,sum(dagen_saldo_tussen_maandstanden) as noemer
,teller/noemer as gem_maand_saldo_bg
from 

(select
	row_nr
	,contract_oid
	,trx_valuta_dt 
	,laatste_dag_vd_maand 
	,saldo_valuta_dt
	, case when dd.aantal_dagen_tot_verval_rte_dag - max(dd.aantal_dagen_tot_verval_rte_dag) OVER (partition by contract_oid,trx_valuta_dt/100 order by trx_valuta_dt rows between 1 following  and 1 following) is null 
           then 	dd.aantal_dagen_tot_verval_rte_dag
           else dd.aantal_dagen_tot_verval_rte_dag - max(dd.aantal_dagen_tot_verval_rte_dag) OVER (partition by contract_oid,trx_valuta_dt/100 order by trx_valuta_dt rows between 1 following  and 1 following)
      end  as dagen_saldo_tussen_maandstanden     
       
 from (select 	row_number() over (partition by trx.contract_oid order by trx.trx_valuta_dt,volg_nr ) as row_nr
				,rek.contract_oid
				,trx.rekeningsoort_nr
				,trx.trx_valuta_dt as trx_valuta_dt
				,trx.volg_nr
				,transactie_bg
				,vorig_contract_saldo_bg
				,(vorig_contract_saldo_bg + transactie_bg) as saldo_valuta_dt
		from s_dm_variabelerente.financiele_transacties trx
		join s_dm_variabelerente.rekening rek
		on trx.contract_oid = rek.contract_oid
		
		qualify row_number() over (partition by trx.contract_oid,trx_valuta_dt order by volg_nr desc) = 1 
		)s
		join (select dim_datum_id,laatste_dag_vd_maand,dag_vd_maand
				,case when dag_vd_maand = 31 then 1
				        when maand_vh_jaar in (1,3,5,7,8,10,12) then laatste_dag_vd_maand - dim_datum_id
				        when maand_vh_jaar in (4,6,9,11) then  (laatste_dag_vd_maand - dim_datum_id)+1
				        when maand_vh_jaar = 2 and schrikkeljaar_ind = 0 then (laatste_dag_vd_maand - dim_datum_id)+3
				        when maand_vh_jaar = 2 and schrikkeljaar_ind = 1 then (laatste_dag_vd_maand - dim_datum_id)+2
				 end    as aantal_dagen_tot_verval_rte_dag
				,schrikkeljaar_ind 
              from  dm_ster.dim_datum dd 
              where 1=1 
              and dim_datum_id between date'2006-12-19' and date
			  ) dd
		on s.trx_valuta_dt = dd.dim_datum_id 
		and dim_datum_id between date'2006-01-01' and date
)s2
group by 1,2
)with data unique primary index(contract_oid, peil_dt);

-- Table met maandstanden en gemiddelde saldi obv renteconevntie 30/260
-- gemiddelde klantrente en referentierente obv eindemaandstanden ((vorige maand + huidige maand) /2). Verfijning later obv renteconventie
-- 
-- obv waarvan de compensatie per maand en cumulatief wordt berekend

drop table s_dm_variabelerente.rekening_compensatie_gem_saldo;

create table s_dm_variabelerente.rekening_compensatie_gem_saldo
as
(select
	 peil_dt
	,contract_oid
	,opening_dt
	,opgeheven_dt
	,gem_maand_saldo_bg
	,SALDO_debetrente_bg
	,avg_klant_rte_jaar_perc
	,marge
	,avg_referentie_rte_jaar_perc
	--,ref_klant_rte_verschil
	,delta
	,compensatie_mnd_bg
	,cum_compensatie_bg
	,compensatie_ropc_mnd_bg
	,cum_compensatie_ropc_bg
	,compensatie_ropr_mnd_bg
	,sum(compensatie_ropr_mnd_bg) OVER (partition by contract_oid order by peil_dt rows unbounded preceding ) as cum_compensatie_ropr_bg
	,compensatie_totaal_mnd_bg
	,cum_compensatie_bg + cum_compensatie_ropc_bg + cum_compensatie_ropr_bg as cum_compensatie_totaal_bg

	from
	(select 
	 peil_dt
	,contract_oid
	,opening_dt
	,opgeheven_dt
	,gem_maand_saldo_bg
	,SALDO_debetrente_bg
	,avg_klant_rte_jaar_perc
	,marge
	,avg_referentie_rte_jaar_perc
	--,ref_klant_rte_verschil
	,delta
	,compensatie_mnd_bg
	,cum_compensatie_bg
	,compensatie_ropc_mnd_bg
	,sum(compensatie_ropc_mnd_bg) OVER (partition by contract_oid order by peil_dt rows unbounded preceding ) as cum_compensatie_ropc_bg
	,((cum_compensatie_ropc_bg - compensatie_ropc_mnd_bg)/12.000) * (avg_klant_rte_jaar_perc/100) as compensatie_ropr_mnd_bg
	,compensatie_totaal_mnd_bg
	,cum_compensatie_bg + cum_compensatie_ropc_bg as cum_compensatie_totaal_bg

	from
	(
	select dim_datum_id as peil_dt
		,cast(contract_oid as decimal(15,0)) as contract_oid
		,opening_dt
		,opgeheven_dt
		,gem_maand_saldo_bg
		,SALDO_debetrente_bg
		,avg_klant_rte_jaar_perc
		,marge
		,avg_referentie_rte_jaar_perc
		,delta
		,cast(compensatie_mnd_bg as decimal(30,2)) as compensatie_mnd_bg
		,sum(compensatie_mnd_bg) OVER (partition by contract_oid order by dim_datum_id rows unbounded preceding ) as cum_compensatie_bg
		,((cum_compensatie_bg - compensatie_mnd_bg)/12.000) * (avg_klant_rte_jaar_perc/100) as compensatie_ropc_mnd_bg
		,compensatie_mnd_bg + compensatie_ropc_mnd_bg as compensatie_totaal_mnd_bg
		from 
			(select 
			 kal.dim_datum_id
			,coalesce(rda.contract_oid, rgs.contract_oid) as contract_oid
			,rgs.gem_maand_saldo_bg
			,rk.SALDO_debetrente_bg
			,rda.opening_dt
			,rda.opgeheven_dt
			,rda.klant_rte_jaar_perc
			,avg(rda.klant_rte_jaar_perc) over (partition by rda.contract_oid order by kal.dim_datum_id rows between 1 preceding and 0 preceding) (decimal(18,5)) as avg_klant_rte_jaar_perc
			,rda.marge
			,rda.referentie_rte_jaar_perc
			,avg(rda.referentie_rte_jaar_perc) over (partition by rda.contract_oid order by kal.dim_datum_id rows between 1 preceding and 0 preceding) (decimal(18,5)) as avg_referentie_rte_jaar_perc
			,avg_referentie_rte_jaar_perc + marge as avg_referentie_rte_incl_marge_jaar_perc
			,(avg_referentie_rte_incl_marge_jaar_perc - avg_klant_rte_jaar_perc)/100.00 as delta
			,cast(((delta * rgs.gem_maand_saldo_bg) /12) as decimal(30,2)) as compensatie_mnd_bg
			from (select dim_datum_id
						,datum_jjjjmmdd
						,laatste_dag_vd_maand as ultimo_maand_dt
						from dm_ster.dim_datum
						where kalender_datum >= '2006-12-19' and kalender_datum < date
						and dim_datum_id = ultimo_maand_dt			-- maandstanden obv laatste dag vd maand
						)kal
			join s_dm_variabelerente.rekening_dagbasis rda
			on rda.peil_dag_dt = kal.dim_datum_id
			left join dwh.sas_rekening_hf rk
			on rda.contract_oid = rk.contract_oid
			and rk.begin_dt <= kal.dim_datum_id
			and (rk.eind_dt > kal.dim_datum_id)
			and rk.geldig_ind = 1
			left join s_dm_variabelerente.rekening_gem_saldo rgs
			on rda.contract_oid = rgs.contract_oid
			and rda.peil_dag_dt = rgs.peil_dt
			where  1=1
			--and rda.contract_oid = 50484844     --TEST
			)T
			
	)T1
)T2
)with data unique primary index(contract_oid,peil_dt)
;

--Finale tabel met per contract de compensatie berekening

drop table s_dm_variabelerente.rekening_compensatie;

create table s_dm_variabelerente.rekening_compensatie
as
(
select
b.contract_oid
,a.opening_dt
,a.opgeheven_dt
,a.cum_compensatie_bg
,a.cum_compensatie_ropc_bg
,a.cum_compensatie_ropr_bg
,a.cum_compensatie_totaal_bg
from  s_dm_variabelerente.rekening_compensatie_gem_saldo a
join s_dm_variabelerente.rekening_klant_segment b
on a.contract_oid = b.contract_oid
where 1=1 
and b.persona = 'ANNA'
--and extract(year from peil_dt)*100+extract(month from peil_dt) <= extract(year from coalesce(a.opgeheven_dt,date'9999-12-31'))*100+extract(month from coalesce(a.opgeheven_dt,date'9999-12-31')) 
and peil_dt =< '2021-08-31'   -- Grens ivm vergelijking voorziening berekening: voor uiteindelijke compensatie deze uitsterren en vorige aanzetten
qualify a.peil_dt = max(a.peil_dt) over (partition by a.contract_oid ) 
)with data unique primary index(contract_oid)
;

-- Controle met oude voorzieningen berekening

sel * from s_dm_variabelerente.rekening_compensatie a
join S_DM_VariabeleRente."voorziening.rente_berekening_agg" b
where 
on a.contract_oid = b.contract_oid
