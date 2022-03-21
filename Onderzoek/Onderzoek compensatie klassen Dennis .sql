select 
--case when compensatie_bg > 5000.00 then  (cast(compensatie_bg as integer)/100) else null end
Case 
when coalesce(compensatie_bg,0) <= 0      then '00: < 0'
when coalesce(compensatie_bg,0) <= 5.00   then '01: <=5'
when coalesce(compensatie_bg,0) <= 10.00  then '02: <=10'
when coalesce(compensatie_bg,0) <= 15.00  then '03: <=15'
when coalesce(compensatie_bg,0) <= 20.00  then '04: <=20'
when coalesce(compensatie_bg,0) <= 25.00  then '05: <=25'
when coalesce(compensatie_bg,0) <= 30.00  then '06: <=30'
when coalesce(compensatie_bg,0) <= 35.00  then '07: <=35'
when coalesce(compensatie_bg,0) <= 40.00  then '08: <=40'
when coalesce(compensatie_bg,0) <= 45.00  then '09: <=45'
when coalesce(compensatie_bg,0) <= 50.00  then '10: <=50'
                            
when coalesce(compensatie_bg,0) <= 60.00  then '11: <=60'
when coalesce(compensatie_bg,0) <= 70.00  then '12: <=70'
when coalesce(compensatie_bg,0) <= 80.00  then '13: <=80'
when coalesce(compensatie_bg,0) <= 90.00  then '14: <=90'
when coalesce(compensatie_bg,0) <= 100.00 then '15: <=100'

when coalesce(compensatie_bg,0) <= 200.00 then '16: <=200'
when coalesce(compensatie_bg,0) <= 300.00 then '17: <=300'
when coalesce(compensatie_bg,0) <= 400.00 then '18: <=400'
when coalesce(compensatie_bg,0) <= 500.00 then '19: <=500'
when coalesce(compensatie_bg,0) <= 600.00 then '20: <=600'
when coalesce(compensatie_bg,0) <= 700.00 then '21: <=700'
when coalesce(compensatie_bg,0) <= 800.00 then '22: <=800'
when coalesce(compensatie_bg,0) <= 900.00 then '23: <=900'
when coalesce(compensatie_bg,0) <= 1000.00 then '24: <=1000'
when coalesce(compensatie_bg,0) <= 1100.00 then '25: <=1100'
when coalesce(compensatie_bg,0) <= 1200.00 then '26: <=1200'
when coalesce(compensatie_bg,0) <= 1300.00 then '27: <=1300'
when coalesce(compensatie_bg,0) <= 1400.00 then '28: <=1400'
when coalesce(compensatie_bg,0) <= 1500.00 then '29: <=1500'
when coalesce(compensatie_bg,0) <= 1600.00 then '30: <=1600'
when coalesce(compensatie_bg,0) <= 1700.00 then '31: <=1700'
when coalesce(compensatie_bg,0) <= 1800.00 then '32: <=1800'
when coalesce(compensatie_bg,0) <= 1900.00 then '33: <=1900'
when coalesce(compensatie_bg,0) <= 2000.00 then '34: <=2000'
when coalesce(compensatie_bg,0) <= 2100.00 then '35: <=2100'
when coalesce(compensatie_bg,0) <= 2200.00 then '36: <=2200'
when coalesce(compensatie_bg,0) <= 2300.00 then '37: <=2300'
when coalesce(compensatie_bg,0) <= 2400.00 then '38: <=2400'
when coalesce(compensatie_bg,0) <= 2500.00 then '39: <=2500'
when coalesce(compensatie_bg,0) <= 2600.00 then '40: <=2600'
when coalesce(compensatie_bg,0) <= 2700.00 then '41: <=2700'
when coalesce(compensatie_bg,0) <= 2800.00 then '42: <=2800'
when coalesce(compensatie_bg,0) <= 2900.00 then '43: <=2900'
when coalesce(compensatie_bg,0) <= 3000.00 then '44: <=3000'
when coalesce(compensatie_bg,0) <= 3100.00 then '45: <=3100'
when coalesce(compensatie_bg,0) <= 3200.00 then '46: <=3200'
when coalesce(compensatie_bg,0) <= 3300.00 then '47: <=3300'
when coalesce(compensatie_bg,0) <= 3400.00 then '48: <=3400'
when coalesce(compensatie_bg,0) <= 3500.00 then '49: <=3500'
when coalesce(compensatie_bg,0) <= 3600.00 then '50: <=3600'
when coalesce(compensatie_bg,0) <= 3700.00 then '51: <=3700'
when coalesce(compensatie_bg,0) <= 3800.00 then '52: <=3800'
when coalesce(compensatie_bg,0) <= 3900.00 then '53: <=3900'
when coalesce(compensatie_bg,0) <= 4000.00 then '54: <=4000'
when coalesce(compensatie_bg,0) <= 4100.00 then '55: <=4100'

when coalesce(compensatie_bg,0) <= 4200.00 then '56: <=4200'
when coalesce(compensatie_bg,0) <= 4300.00 then '57: <=4300'
when coalesce(compensatie_bg,0) <= 4400.00 then '58: <=4400'
when coalesce(compensatie_bg,0) <= 4500.00 then '59: <=4500'
when coalesce(compensatie_bg,0) <= 4600.00 then '60: <=4600'
when coalesce(compensatie_bg,0) <= 4700.00 then '61: <=4700'
when coalesce(compensatie_bg,0) <= 4800.00 then '62: <=4800'
when coalesce(compensatie_bg,0) <= 4900.00 then '63: <=4900'
when coalesce(compensatie_bg,0) <= 5000.00 then '64: <=5000'
--when compensatie_bg > 5000.00 then '65: > 5000'

when compensatie_bg > 5000.00 then '65: >='||Cast((cast(compensatie_bg as integer)/100) as varchar(10))||'00'
end
,ccl.contract_rol_cd
,d.decode_omschr
,sum(compensatie_bg) as compensatie_bg
,count(distinct contract_oid) as aantal_rekeningen 

from S_DM_VariabeleRente.rekening rek
left join dwh.contract_contract_l ccl
on rek.contract_oid = ccl.relateert_aan_contract_oid
and ccl.eind_dt = '9999-12-31' and ccl.geldig_ind = 1
and ccl.relatie_eind_dt is null
left join dwh.contract_h chu
on ccl.gerelateerd_contract_oid = chu.contract_oid
left join dwh.decode d
on d.decode_oid = ccl.contract_rol_cd_oid

--left join S_DM_VariabeleRente.rente_berekening_agg com
--on rek.contract_oid = com.contract_oid
left join s_dm_variabelerente.rekening_compensatie com
on rek.contract_oid = com.contract_oid
group by 1,2
order by 1,2;


--deze is voor intern en externe rekeningen

select rek.contract_oid
,rek.rek_nr
,ccl. contract_rol_cd
,d.decode_omschr
,chu.bron_contract_2_nr as IBAN_nr
from S_DM_VariabeleRente.rekening rek
left join dwh.contract_contract_l ccl
on rek.contract_oid = ccl.relateert_aan_contract_oid
and ccl.eind_dt = '9999-12-31' and ccl.geldig_ind = 1
and ccl.relatie_eind_dt is null
left join dwh.contract_h chu
on ccl.gerelateerd_contract_oid = chu.contract_oid
left join dwh.decode d
on d.decode_oid = ccl.contract_rol_cd_oid
where ccl.contract_rol_cd is not null
group by 1,2,3
order by 1,2,3









