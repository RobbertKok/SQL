select a.blokkeringsreden_cd,
b.decode_omschr
,count(*)
from dwh.sas_klantblokkering a 
join dwh.sas_decode b
on a.blokkeringsreden_cd_oid = b.decode_oid
group by 1,2
order by 1,2;