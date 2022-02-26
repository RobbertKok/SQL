select 
case when compensatie_bg > 5000.00 then  (cast(compensatie_bg as integer)/100) else null end
,Case 
when compensatie_bg <= 0      then '00: < 0'
when compensatie_bg <= 5.00   then '01: <=5'
when compensatie_bg <= 10.00  then '02: <=10'
when compensatie_bg <= 15.00  then '03: <=15'
when compensatie_bg <= 20.00  then '04: <=20'
when compensatie_bg <= 25.00  then '05: <=25'
when compensatie_bg <= 30.00  then '06: <=30'
when compensatie_bg <= 35.00  then '07: <=35'
when compensatie_bg <= 40.00  then '08: <=40'
when compensatie_bg <= 45.00  then '09: <=45'
when compensatie_bg <= 50.00  then '10: <=50'
                            
when compensatie_bg <= 60.00  then '11: <=60'
when compensatie_bg <= 70.00  then '12: <=70'
when compensatie_bg <= 80.00  then '13: <=80'
when compensatie_bg <= 90.00  then '14: <=90'
when compensatie_bg <= 100.00 then '15: <=100'

when compensatie_bg <= 200.00 then '16: <=200'
when compensatie_bg <= 300.00 then '17: <=300'
when compensatie_bg <= 400.00 then '18: <=400'
when compensatie_bg <= 500.00 then '19: <=500'
when compensatie_bg <= 600.00 then '20: <=600'
when compensatie_bg <= 700.00 then '21: <=700'
when compensatie_bg <= 800.00 then '22: <=800'
when compensatie_bg <= 900.00 then '23: <=900'
when compensatie_bg <= 1000.00 then '24: <=1000'
when compensatie_bg <= 1100.00 then '25: <=1100'
when compensatie_bg <= 1200.00 then '26: <=1200'
when compensatie_bg <= 1300.00 then '27: <=1300'
when compensatie_bg <= 1400.00 then '28: <=1400'
when compensatie_bg <= 1500.00 then '29: <=1500'
when compensatie_bg <= 1600.00 then '30: <=1600'
when compensatie_bg <= 1700.00 then '31: <=1700'
when compensatie_bg <= 1800.00 then '32: <=1800'
when compensatie_bg <= 1900.00 then '33: <=1900'
when compensatie_bg <= 2000.00 then '34: <=2000'
when compensatie_bg <= 2100.00 then '35: <=2100'
when compensatie_bg <= 2200.00 then '36: <=2200'
when compensatie_bg <= 2300.00 then '37: <=2300'
when compensatie_bg <= 2400.00 then '38: <=2400'
when compensatie_bg <= 2500.00 then '39: <=2500'
when compensatie_bg <= 2600.00 then '40: <=2600'
when compensatie_bg <= 2700.00 then '41: <=2700'
when compensatie_bg <= 2800.00 then '42: <=2800'
when compensatie_bg <= 2900.00 then '43: <=2900'
when compensatie_bg <= 3000.00 then '44: <=3000'
when compensatie_bg <= 3100.00 then '45: <=3100'
when compensatie_bg <= 3200.00 then '46: <=3200'
when compensatie_bg <= 3300.00 then '47: <=3300'
when compensatie_bg <= 3400.00 then '48: <=3400'
when compensatie_bg <= 3500.00 then '49: <=3500'
when compensatie_bg <= 3600.00 then '50: <=3600'
when compensatie_bg <= 3700.00 then '51: <=3700'
when compensatie_bg <= 3800.00 then '52: <=3800'
when compensatie_bg <= 3900.00 then '53: <=3900'
when compensatie_bg <= 4000.00 then '54: <=4000'
when compensatie_bg <= 4100.00 then '55: <=4100'

when compensatie_bg <= 4200.00 then '56: <=4200'
when compensatie_bg <= 4300.00 then '57: <=4300'
when compensatie_bg <= 4400.00 then '58: <=4400'
when compensatie_bg <= 4500.00 then '59: <=4500'
when compensatie_bg <= 4600.00 then '60: <=4600'
when compensatie_bg <= 4700.00 then '61: <=4700'
when compensatie_bg <= 4800.00 then '62: <=4800'
when compensatie_bg <= 4900.00 then '63: <=4900'
when compensatie_bg <= 5000.00 then '64: <=5000'
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

left join S_DM_VariabeleRente.rente_berekening_agg com
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









