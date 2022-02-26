create table s_dm_cdo.contract_analyse_per_maand
as (
select 
ultimo_maand_dt
,vr.contract_oid
,case when opgeheven_dt > ultimo_maand_dt then 'actief'else 'opgeheven' end as actief_ind 
,sum(kredietlimiet_bg) as kredietlimiet_bg
,sum(saldo_bg) as saldo_bg
--,count(distinct vr.contract_oid) as aant_rekeningen
--,sum(case when opgeheven_dt < ultimo_maand_dt then 1 else 0 end) as aant_opgeheven
,sum(case when opgeheven_dt > ultimo_maand_dt then kredietlimiet_bg else null end) as kredietlimiet_bg_actieve_contracten
,sum(case when opgeheven_dt > ultimo_maand_dt then saldo_bg else null end) as saldo_bg_actieve_contracten


from
(select dim_datum_id
,datum_jjjjmmdd
,laatste_dag_vd_maand as ultimo_maand_dt
,dag_vd_maand
from dm_ster.dim_datum
where kalender_datum >= '2007-01-01' and kalender_datum < date
and dim_datum_id = laatste_dag_vd_maand
)kal
cross join s_dm_variabelerente.rekening vr
left join dwh.kredietlimiet_s ks
on vr.contract_oid = ks.contract_oid
and ks.kredietlimiet_start_dt <= kal.dim_datum_id
and (ks.kredietlimiet_eind_dt > kal.dim_datum_id or ks.kredietlimiet_eind_dt is null)
and ks.begin_dt <= kal.dim_datum_id
and ks.eind_dt > kal.dim_datum_id
and ks.limietsoort_cd = 30
left join dwh.contract_saldo_s sal
on vr.contract_oid = sal.contract_oid
and sal.saldo_start_dt <= kal.dim_datum_id
and (sal.saldo_eind_dt > kal.dim_datum_id or sal.saldo_eind_dt is null)
and sal.begin_dt <= kal.dim_datum_id
and sal.eind_dt > kal.dim_datum_id
and sal.geldig_ind = 1
group by 1,2,3
) with data unique primary index(ultimo_maand_dt,contract_oid);
