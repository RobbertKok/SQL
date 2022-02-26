select a.blokkeringsreden_cd,
b.decode_omschr
,count(*)
from dwh.sas_klantblokkering a 
join dwh.sas_decode b
on a.blokkeringsreden_cd_oid = b.decode_oid
WHERE a.eind_dt = '9999-12-31' and a.geldig_ind = 1
group by 1,2
order by 1,2;