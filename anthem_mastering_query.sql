with
project_selection as(
select Client_Name__c               
                  ,project_name__c               
                  ,count(1) count_recs                
                from Provider__c               
               where 1=1
                 and project_name__c = 'Anthem Medicaid|NY 2018|MRR|03180252'
                 and client_name__c like 'Anthem%' and project_name__c like '%2018%MRR%'                           
            group by Client_Name__c               
                    ,project_name__c)

--5996 rows

,provider_loc_reference as(
select ProviderLocationJunction__c.provider__c
                    ,ProviderLocationJunction__c.location__c
                    ,row_number() over(partition by ProviderLocationJunction__c.location__c
                                                   ,ProviderLocationJunction__c.Provider__c
                                           order by ProviderLocationJunction__c.LastModifiedDate)
                     as rownum_remove_lid_dupes_for_a_provider 
                    ,case when isnull(ProviderLocationJunction__c.LastModifiedDate,ProviderLocationJunction__c.CreatedDate ) > ProviderLocationJunction__c.CreatedDate
                          then ProviderLocationJunction__c.LastModifiedDate
                          else ProviderLocationJunction__c.CreatedDate
                      end as plj_record_date
                from project_selection   
                join ProviderLocationJunction__c
				on project_selection.project_name__c = ProviderLocationJunction__c.project_name__c)

--16704 without duplicate removal
--select count(*) 
--into #provider_loc_reference 
--from provider_loc_reference where rownum_remove_lid_dupes_for_a_provider=1

--6005 distinct records

,rel_qap_data as (
              select provider_loc_reference.Location__c
                    ,provider_loc_reference.Provider__c       
                    ,provider_loc_reference.plj_record_date
                    ,row_number() over(partition by provider_loc_reference.location__c
                                           order by plj_record_date)
                     as rownum_remove_location_repeatition_forMasterProcessing
                    ,row_number() over(partition by provider_loc_reference.location__c
                                                   ,provider_loc_reference.Provider__c
                                           order by case when isnull(Chart__c.LastModifiedDate,Chart__c.CreatedDate ) > Chart__c.CreatedDate
                                                         then Chart__c.LastModifiedDate
                                                         else Chart__c.CreatedDate
                                                     end desc)
                     as rownum_remove_chartLvl_reptions_for_cl_processing
                    ,count(provider_loc_reference.Provider__c) over( partition by provider_loc_reference.location__c)
                     as provider_charts_at_loc
                    ,Chart__c.Appointment_Type__c
                    ,count(Chart__c.id) over( partition by Chart__c.Location__c,Appointment_Type__c)
                     as countOfChartsAtLocationbyAppointmentType
                           --,row_number() over(partition by provider_loc_reference.Provider__c
                  --                       order by plj_record_date)
                  -- as rownum_remove_provider_repeatition
                        from provider_loc_reference
                join Chart__c
                  on (provider_loc_reference.Location__c     = Chart__c.Location__c
                      and provider_loc_reference.Provider__c = Chart__c.Provider__c)
                 and FileBound_Disposition_Code__c = 'QAP'
                 and FileBound_Status__c = 'REL'
                 and MRT_Name__c <> 'Replication'
                 and provider_loc_reference.rownum_remove_lid_dupes_for_a_provider = 1 )

select rel_qap_data.* into #rel_qap_data
                         from rel_qap_data

--31671 rows affected




