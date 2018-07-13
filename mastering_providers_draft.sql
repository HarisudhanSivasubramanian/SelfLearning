select * from dw.poc__npiRegistry_npi_Deactivation_List where npi = '1003002031'

select npi__C,Client_Name__c, Project_Name__c,* from dbo.Provider__c where npi__c = '1003002031';

with npiTest as(
select n.NPI as npi_npi,p.npi__c as provider_npi,d.npi, p.Project_name__c, 
           p.Client_name__c,
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
select client_name__c,npi_npi,provider_npi,Status_of_npi,
Taxonomy_GRP1,
Taxonomy_GRP2,
Taxonomy_code1,
Taxonomy_code2
from npiTest
where npi_npi = '1003892894'
where rownumber = 1

--546445 rows of NPIs across all the clients with there Entity_type_status

--select * from dw.poc__npiRegistry_npi_Full_Dump where npi = '1114064250'

--select * from poc__npiRegistry_npi_Full_Dump where npi = '1225366610'

--select * from provider__c where npi__c = '1225366610'

--select * from poc__npiRegistry_npi_Deactivation_List where npi = '1225366610'

with npiTest as(
select n.NPI as npi_npi,p.npi__c as provider_npi,d.npi, p.Project_name__c, 
           p.Client_name__c,
		   p.Full_Name__c,p.Provider_Full_Address_With_Suite__c,p.Phone_Number__c,p.Fax_Number__c,
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
where npi_npi = '1003002031'
and rownumber = 1


select npi__c, count(1), count(distinct Project_Name__c)
   from provider__c
  where CreatedDate > '2015'
group by npi__c
 having count(1) > 10

 select npi__C,client_name__C, count(1)
 from provider__c
 where npi__c = '1003892894'
 group by NPI__c,Client_Name__c

 select * from provider__c where npi__c = '1003892894'

with npiTest as(
select n.NPI as npi_npi,p.npi__c as provider_npi,d.npi, p.Project_name__c, 
           p.Client_name__c,
		   p.Full_Name__c,p.Provider_Full_Address_With_Suite__c,p.Phone_Number__c,p.Fax_Number__c,p.Secondary_Phone_Number__c,
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
where npi_npi = '1376525824'
--and rownumber = 1

