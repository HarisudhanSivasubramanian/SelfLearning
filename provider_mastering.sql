SELECT
  Client_Name__c,
  project_name__c,
  COUNT(1) count_recs
FROM Provider__c
WHERE 1 = 1 
and npi__c is not null
and Project_status__c <> 'Inactive'
GROUP BY Client_Name__c,
         project_name__c
order by Client_Name__c,
         project_name__c

--list all details for a project which is active and NPI is not null
select npi__c,full_name__c,phone_number__c,Provider_Full_Address_With_Suite__c,city__c,state__c from provider__c
where project_name__c = 'UHG MD|EPSDT 2018|MRR|03180274'
and Project_status__c <> 'Inactive'
and npi__c is not null

SELECT
  p.npi__c,
  p.full_name__c,
  p.phone_number__c,
  p.Provider_Full_Address_With_Suite__c,
  p.city__c,
  p.state__c,
  p.createddate,
  case when f.entity_type_code = 1 then 'Individual' 
       when f.entity_type_code = 2 then 'Organization'
	   Else 'No Record' end,
  --f.entity_type_code,
  f.NPI,
  f.Provider_first_line_Business_Mailing_Address,
  f.Provider_Second_Line_Business_Mailing_Address
FROM [dbo].[poc__npiRegistry_npi_Full_Dump] f 
join  provider__c p
   ON f.npi = p.npi__c 
WHERE p.project_name__c = 'AppleCare|SPC EMR 2018|MRR|03180534'
AND p.Project_status__c <> 'Inactive'
AND p.npi__c IS NOT NULL

select * from [dbo].[poc__npiRegistry_npi_Full_Dump] where NPI = '1891898797'

select * from provider__c where npi__c = '1891898797'


--1891898797

--Name, External_identifier_c and provider_c values are different for this NPI 1891898797


select * from ProviderLocationJunction__c
where project_name__c = 'Aetna|2015 Commercial Phase 1|MRR|03150819'


select npi__c, count(1), count(distinct Project_Name__c)
   from provider__c
  where CreatedDate > '2015'
group by npi__c
 having count(1) > 10

select npi__c, count(1), count(distinct Project_Name__c)
   from provider__c
  where CreatedDate > '2015'
group by npi__c
 having count(distinct Project_Name__c) > 10

select * from provider__c where npi__c = '1003892894'

select * from poc__npiRegistry_npi_full_dump where npi = '1003892894'

select client_name__c, project_name__c,count(1)
from provider__c where npi__c = '1003892894'
group by client_name__c
order by 2

select project_name__c,client_name__c,count(1)
from provider__c where npi__c = '1003892894'
group by project_name__c,client_name__c
order by 3

select npi__c,
       provider_id__c,
	   project_name__c,
	   client_name__c,
	   * 
from   provider__c 
where  npi__c = '1003892894' and provider_id__c is not null
order by 3,4

select * from poc__npiRegistry_npi_full_dump where npi = '1003892894'

with npiTest as (select p.npi__c from provider__c p
join poc__npiRegistry_npi_full_dump f on p.npi__c = f.NPI
--join poc__npiRegistry_npi_Deactivation_List d on f.npi = d.npi
where project_name__c = 'BSCA|Commercial 2016|MRR|03160987'
and client_name__c = 'Blue Shield of California'
and f.entity_type_code is null)
select count(npi__c) from npiTest n
join poc__npiRegistry_npi_Deactivation_List d
on n.npi__c = d.npi

--148 records

select p.npi__c from provider__c p
join poc__npiRegistry_npi_full_dump f on p.npi__c = f.NPI
--join poc__npiRegistry_npi_Deactivation_List d on f.npi = d.npi
where project_name__c = 'BSCA|Commercial 2016|MRR|03160987'
and client_name__c = 'Blue Shield of California'
and f.entity_type_code is null

--148 records


select * from [dw].[poc__npiRegistry_npi_Deactivation_List]


with npiTest as(
select n.NPI as npi_npi,p.npi__c as provider_npi,d.npi, p.Project_name__c, p.Client_name__c,
       case when n.entity_type_code = 1 then 'Individual'
	        when n.entity_type_code = 2 then 'Organization'
			else 'De-activated' end as Status
from Provider__c p
join poc__npiRegistry_npi_full_dump n on p.npi__c = n.npi
left join poc__npiRegistry_npi_Deactivation_List d on n.npi = d.npi
)
select project_name__c,client_name__c,status,count(1)
from npiTest
group by project_name__c,client_name__c,status
order by project_name__c,client_name__c,status
