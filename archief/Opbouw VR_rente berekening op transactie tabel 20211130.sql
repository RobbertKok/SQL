

replace recursive view s_dm_cdo.vr_rente_berekening (row_nr, peil_dt,contract_oid, rek_nr, betaalderente, gecorrigeerdeRente, RenteBedragVerschil, saldo, gecorrigeerdsaldo
, ReferentieRentePlusMarge
) as
(
select rt.row_nr
, rt.peil_dt
, r.contract_oid
, r.rek_nr
, betaalde_rente_bg as BetaaldeRente
, betaalde_rente_bg as GecorrigeerdeRente
, CAST(0 as decimal(18,6)) AS RenteBedragVerschil
, saldo_voor_rte_boeking_bg AS Saldo
, saldo_voor_rte_boeking_bg AS GecorrigeerdSaldo
, CAST(rr.referentie_rente AS decimal(18,6)) + m.marge
from s_dm_cdo.vr_rekening_transactie rt
join s_dm_cdo.vr_rekening r on rt.contract_oid = r.contract_oid
join s_dm_cdo.vr_referentie_rente rr ON rr.maand = rt.peil_dt
join s_dm_cdo.vr_marge_per_contract m on r.contract_oid = m.contract_oid
where row_nr=1
union all
select rt.row_nr
, rt.peil_dt
, r.contract_oid
, r.rek_nr
, rt.betaalde_rente_bg as BetaaldeRente
, (a.GecorrigeerdSaldo - (a.saldo - rt.saldo_voor_rte_boeking_bg) + RenteBedragVerschil)
*((CAST(rr.referentie_rente AS decimal(18,6)) + m.marge)/12)
as GecorrigeerdeRente
, CAST( (a.GecorrigeerdSaldo - (a.saldo - rt.saldo_voor_rte_boeking_bg) + RenteBedragVerschil)
*((CAST(rr.referentie_rente AS decimal(18,6)) + m.marge)/12)
-
rt.betaalde_rente_bg as decimal(18,6)) AS RenteBedragVerschil
, rt.saldo_voor_rte_boeking_bg AS Saldo
, a.GecorrigeerdSaldo - (a.saldo - rt.saldo_voor_rte_boeking_bg) + RenteBedragVerschil AS GecorrigeerdSaldo
, CAST(rr.referentie_rente AS decimal(18,6)) + m.marge as ReferentieRentePlusMarge
from vr_rente_berekening as a
join s_dm_cdo.vr_rekening_transactie rt on a.contract_oid = rt.contract_oid
join s_dm_cdo.vr_rekening r on rt.contract_oid = r.contract_oid
join s_dm_cdo.vr_referentie_rente rr ON rr.maand = rt.peil_dt
join s_dm_cdo.vr_marge_per_contract m on r.contract_oid = m.contract_oid
where rt.row_nr = a.row_nr+1
and r.contract_oid = a.contract_oid)
;

drop table s_dm_cdo.vr_rente_berekening_t;

create table s_dm_cdo.vr_rente_berekening_t
as
(select * from s_dm_cdo.vr_rente_berekening) with data unique primary index(contract_oid, row_nr)
;

drop table s_dm_cdo.vr_rente_berekening_agg;

create table s_dm_cdo.vr_rente_berekening_agg
as
(
sel contract_oid
,sum(RenteBedragVerschil) as compensatie_bg
from s_dm_cdo.vr_rente_berekening_t
group by 1
)with data unique primary index(contract_oid);



