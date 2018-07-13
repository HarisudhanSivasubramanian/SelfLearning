select npi__c, count(1), count(distinct Project_Name__c)
   from provider__c
  where CreatedDate > '2015'
group by npi__c
 having count(1) > 10

--Aetna|Commercial 2017|MRR|03170932



select npi__c, count(1), count(distinct Project_Name__c)
   from provider__c
  where CreatedDate > '2015'
group by npi__c
 having count(distinct Project_Name__c) > 10

select * from provider__c where npi__c = '1003892894'

select * from dw.poc__npiRegistry_npi_full_dump where npi = '1003892894'

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

select count(distinct npi) from poc__npiRegistry_npi_full_dump where entity_type_code is not null

--healthcare_provider_taxonomy_code_1

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
			else 'De-activated' end as Status_of_npi,
			ROW_NUMBER() OVER (PARTITION BY p.client_name__c,p.project_name__C  ORDER BY p.project_name__c) as rownumber
from Provider__c p
join poc__npiRegistry_npi_full_dump n on p.npi__c = n.npi
left join poc__npiRegistry_npi_Deactivation_List d on n.npi = d.npi
)
select project_name__c,client_name__c,npi_npi,Status_of_npi
from npiTest
where client_name__c <> 'Episource (Internal)'
group by project_name__c,client_name__c,status
order by project_name__c,client_name__c,status


--Healthcare_Provider_Taxonomy_Code_1

select * from Chart__c

--Health_plan_ID__c
--mcc_provider_npi__c

select * from poc__npiRegistry_npi_full_dump where npi = '1003892894'

--healthcare_provider_taxonomy_code_1

select top 10 npi,healthcare_provider_taxonomy_code_1,
healthcare_provider_taxonomy_code_2,
isnull(healthcare_provider_taxonomy_code_1,healthcare_provider_taxonomy_code_2)
from poc__npiRegistry_npi_full_dump
where entity_type_code is not null
--and healthcare_provider_taxonomy_code_1 is null

select * from poc__npiRegistry_npi_full_dump where npi = '1164431508'

select * from [dw].[poc__npiRegistry_Taxonomy_Code_Set] where taxonomy_code in ('101YP2500X','106H00000X')

select npi__c,*
from provider__c
where project_name__c = 'Aetna|Commercial 2017|MRR|03170932'

select * from poc__npiRegistry_npi_full_dump where npi = '1649271453'

select npi__c,client_name__c, project_name__c,* from provider__c where npi__c = '1649271453'
order by 2,3;

with npiTest as(select npi__c,client_name__c, project_name__c,
ROW_NUMBER() OVER (PARTITION BY client_name__c,project_name__C  ORDER BY project_name__c) as rownumber
from provider__c
where npi__c = '1649271453')
select * from npiTest where rownumber = 1

select npi,entity_type_code,
--Healthcare_Provider_Taxonomy_Code_1,Healthcare_Provider_Primary_Taxonomy_Switch_1,Healthcare_Provider_Taxonomy_Code_2,Healthcare_Provider_Primary_Taxonomy_Switch_2,Healthcare_Provider_Taxonomy_Code_3,Healthcare_Provider_Primary_Taxonomy_Switch_3,Healthcare_Provider_Taxonomy_Code_4,Healthcare_Provider_Primary_Taxonomy_Switch_4,Healthcare_Provider_Taxonomy_Code_5,Healthcare_Provider_Primary_Taxonomy_Switch_5,Healthcare_Provider_Taxonomy_Code_6,Healthcare_Provider_Primary_Taxonomy_Switch_6,Healthcare_Provider_Taxonomy_Code_7,Healthcare_Provider_Primary_Taxonomy_Switch_7,Healthcare_Provider_Taxonomy_Code_8,Healthcare_Provider_Primary_Taxonomy_Switch_8,Healthcare_Provider_Taxonomy_Code_9,Healthcare_Provider_Primary_Taxonomy_Switch_9,Healthcare_Provider_Taxonomy_Code_10,Healthcare_Provider_Primary_Taxonomy_Switch_10,Healthcare_Provider_Taxonomy_Code_11,Healthcare_Provider_Primary_Taxonomy_Switch_11,Healthcare_Provider_Taxonomy_Code_12,Healthcare_Provider_Primary_Taxonomy_Switch_12,Healthcare_Provider_Taxonomy_Code_13,Healthcare_Provider_Primary_Taxonomy_Switch_13,Healthcare_Provider_Taxonomy_Code_14,Healthcare_Provider_Primary_Taxonomy_Switch_14,Healthcare_Provider_Taxonomy_Code_15,Healthcare_Provider_Primary_Taxonomy_Switch_15,
Healthcare_Provider_Taxonomy_Group_1,Healthcare_Provider_Taxonomy_Group_2,Healthcare_Provider_Taxonomy_Group_3,Healthcare_Provider_Taxonomy_Group_4,Healthcare_Provider_Taxonomy_Group_5,Healthcare_Provider_Taxonomy_Group_6,Healthcare_Provider_Taxonomy_Group_7,Healthcare_Provider_Taxonomy_Group_8,Healthcare_Provider_Taxonomy_Group_9,Healthcare_Provider_Taxonomy_Group_10,Healthcare_Provider_Taxonomy_Group_11,Healthcare_Provider_Taxonomy_Group_12,Healthcare_Provider_Taxonomy_Group_13,Healthcare_Provider_Taxonomy_Group_14,Healthcare_Provider_Taxonomy_Group_15
from poc__npiRegistry_npi_Full_Dump

--Wherever the taxonomy group values are there, its not in provider__c data


with npiTest as(
select n.NPI as npi_npi,p.npi__c as provider_npi,d.npi, p.Project_name__c, 
           p.Client_name__c,
		   p.Full_Name__c,p.Provider_Full_Address_With_Suite__c,
		   --p.Phone_Number__c,
		   isnull('('+Stuff(Stuff(p.Phone_Number__c,7,0,'-'),4,0,')'),0) as Phone_number,
		   isnull('('+Stuff(Stuff(p.Fax_Number__c,7,0,'-'),4,0,')'),0) as Fax_number,
		   --p.Fax_Number__c,
		   isnull('('+Stuff(Stuff(p.Secondary_Phone_Number__c,7,0,'-'),4,0,')'),'Not available') as Secondary_phone_number,
		   --p.Secondary_Phone_Number__c,
		   p.City__c,p.State__c,p.ZIP_Code__c,p.Provider_Specialty__c,
		   n.Healthcare_Provider_Taxonomy_Group_1 as Taxonomy_GRP1,
		   n.Healthcare_Provider_Taxonomy_Group_2 as Taxonomy_Grp2,
		   n.Healthcare_Provider_Taxonomy_Code_1 as Taxonomy_code1,
		   n.Healthcare_Provider_Taxonomy_Code_2 as Taxonomy_code2,
       case when n.entity_type_code = 1 then 'Individual'
	        when n.entity_type_code = 2 then 'Organization'
			else 'De-activated' end as Status_of_npi,
			ROW_NUMBER() OVER (PARTITION BY p.client_name__c,n.npi  ORDER BY p.client_name__c) as rownumber
from poc__npiRegistry_npi_full_dump n
join provider__c p on  p.npi__c = n.npi
left join poc__npiRegistry_npi_Deactivation_List d on n.npi = d.npi
)
select *
from npiTest
where npi_npi = '1871502229'
--and rownumber = 1