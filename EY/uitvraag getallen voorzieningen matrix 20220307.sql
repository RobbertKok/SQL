select  
r.contract_oid
,opening_dt
,opgeheven_dt
,l.partij_oid
,k.klant_uitstroom_dt

from s_dm_variabelerente.rekening r
join S_DM_VariabeleRente.klant_rekening_rol l
on r.contract_oid = l.contract_oid
and l.CONTRACT_PARTIJ_ROL_CD = 1
join S_DM_VariabeleRente.klant k
on l.partij_oid = k.partij_oid
where r.REK_SRT <> 615 
