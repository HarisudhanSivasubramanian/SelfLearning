if object_id('tempdb..#rel_qap_data')                  is not null drop table #rel_qap_data
if object_id('tempdb..#poc__location_master_hash_gen') is not null drop table #poc__location_master_hash_gen 
 
go
--(A) PROJECT SELECTION
                with 
project_selection as (
              select Client_Name__c                
                     ,project_name__c                
                     ,count(1) count_recs                 
                from Provider__c                
               where 1=1
                 and project_name__c = 'Aetna|Commercial 2017|MRR|03171048'
                 and client_name__c like 'Aetna%' and project_name__c like '%2017%MRR%'                            
            group by Client_Name__c                
                    ,project_name__c)   --select top 10 * from project_selection

   
 --(B) From PL-Jn, prepare to remove LID duplicates at provider level & Collect Provider / LID combinations    
,provider_loc_reference as ( 
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
                  on project_selection.project_name__c = ProviderLocationJunction__c.project_name__c)   --select * into #provider_loc_reference from provider_loc_reference where rownum_remove_lid_dupes_for_a_provider=1 --(220638 row(s) affected)
   
    ,rel_qap_data as (
              select provider_loc_reference.Location__c
                    ,provider_loc_reference.Provider__c
                    ,provider_loc_reference.plj_record_date 
                                  ,Chart__c.id chart_id
                                  ---
                    ,provider__c.NPI__c          
                    ,provider__c.TIN_ID__c
                                  ,provider__c.Provider_Full_Address_With_Suite__c
                                  ,provider__c.City__c
                                  ,State__c
                                  ,ZIP_Code__c 
                                  ,provider__c.Phone_Number__c
                                  ,Secondary_Phone_Number__c
                    ---
                                  ,provider__c.Provider_Full_Address_With_Suite__c + ' | ' + provider__c.City__c  + ' | ' + State__c  + ' | '  + ZIP_Code__c AS FULL_ADDRESS
                                  ,isnull(provider__c.Phone_Number__c,Secondary_Phone_Number__c) Phone_Number
                                  ---
                    ,row_number() 
                                   over(partition by provider_loc_reference.location__c
                              order by plj_record_date) 
                                   as rownum_remove_location_repeatition_forMasterProcessing
                    ,row_number() 
                                   over(partition by provider_loc_reference.location__c
                                      ,provider_loc_reference.Provider__c
                              order by case when isnull(Chart__c.LastModifiedDate,Chart__c.CreatedDate ) > Chart__c.CreatedDate 
                                            then Chart__c.LastModifiedDate
                                            else Chart__c.CreatedDate
                                        end desc) --sorting by chart dates, actually not required when we are intrested in removing chart info and go to location,provider level
                     as rownum_remove_chartLvl_reptions_for_cl_processing
                    ---
                    ,count(Chart__c.id) 
                                   over( partition by provider_loc_reference.location__c) 
                                   as charts_at_loc
                                  ,count(chart__c.id) 
                                   over (partition by provider__c.NPI__c
                                                     ,provider__c.TIN_ID__c
                                                                 ,provider__c.Provider_Full_Address_With_Suite__c + ' | ' + provider__c.City__c  + ' | ' + State__c  + ' | '  + ZIP_Code__c
                                                                 ,isnull(provider__c.Phone_Number__c,Secondary_Phone_Number__c)
                                                                 ,provider_loc_reference.Location__c) 
                              as CountOfCharts_atLoc_byNpiTinAddPhone
                                  ,count(chart__c.id) 
                                   over (partition by provider__c.NPI__c
                                                     ,provider__c.Provider_Full_Address_With_Suite__c + ' | ' + provider__c.City__c  + ' | ' + State__c  + ' | '  + ZIP_Code__c
                                                                 ,isnull(provider__c.Phone_Number__c,Secondary_Phone_Number__c)
                                                                 ,provider_loc_reference.Location__c) 
                              as CountOfCharts_atLoc_byNpiAddPhone                    
                                  ,count(chart__c.id) 
                                   over (partition by provider__c.NPI__c
                                                                 ,isnull(provider__c.Phone_Number__c,Secondary_Phone_Number__c)
                                                                 ,provider_loc_reference.Location__c) 
                              as CountOfCharts_atLoc_byNpiPhone
                                  ,count(chart__c.id) 
                                   over (partition by provider__c.NPI__c
                                                     ,provider__c.Provider_Full_Address_With_Suite__c + ' | ' + provider__c.City__c  + ' | ' + State__c  + ' | '  + ZIP_Code__c
                                                                 ,provider_loc_reference.Location__c) 
                              as CountOfCharts_atLoc_byNpiAdd
                                  ,count(chart__c.id) 
                                   over (partition by provider__c.TIN_ID__c
                                                                 ,provider__c.Provider_Full_Address_With_Suite__c + ' | ' + provider__c.City__c  + ' | ' + State__c  + ' | '  + ZIP_Code__c
                                                                 ,isnull(provider__c.Phone_Number__c,Secondary_Phone_Number__c)
                                                                 ,provider_loc_reference.Location__c) 
                              as CountOfCharts_atLoc_byTinAddPhone
                                  ,count(chart__c.id) 
                                   over (partition by provider__c.TIN_ID__c
                                                                 ,isnull(provider__c.Phone_Number__c,Secondary_Phone_Number__c)
                                                                 ,provider_loc_reference.Location__c) 
                              as CountOfCharts_atLoc_byTinPhone
                                  ,count(chart__c.id) 
                                   over (partition by provider__c.TIN_ID__c
                                                                 ,provider__c.Provider_Full_Address_With_Suite__c + ' | ' + provider__c.City__c  + ' | ' + State__c  + ' | '  + ZIP_Code__c
                                                                 ,provider_loc_reference.Location__c) 
                              as CountOfCharts_atLoc_byTinAdd
                                  ,count(chart__c.id) 
                                   over (partition by isnull(provider__c.Phone_Number__c,Secondary_Phone_Number__c)
                                                                 ,provider_loc_reference.Location__c) 
                              as CountOfCharts_atLoc_byPhone
                                  ,count(chart__c.id) 
                                   over (partition by provider__c.Provider_Full_Address_With_Suite__c + ' | ' + provider__c.City__c  + ' | ' + State__c  + ' | '  + ZIP_Code__c
                                                                 ,provider_loc_reference.Location__c) 
                              as CountOfCharts_atLoc_byAddress
                                  ---
                    ,Chart__c.Appointment_Type__c
                --  ,count(Chart__c.id) over( partition by Chart__c.Location__c,Appointment_Type__c) 
                from provider_loc_reference
                join Chart__c 
                  on (provider_loc_reference.Location__c     = Chart__c.Location__c
                      and provider_loc_reference.Provider__c = Chart__c.Provider__c)
                         join Provider__c
                             on provider_loc_reference.Provider__c = Provider__c.id
               where FileBound_Disposition_Code__c = 'QAP'
                 and FileBound_Status__c = 'REL' 
                 and MRT_Name__c <> 'Replication'
                 and provider_loc_reference.rownum_remove_lid_dupes_for_a_provider = 1 )

              select * into #rel_qap_data from rel_qap_data
                             --(376029 row(s) affected)
                 --(352135 row(s) affected)  Post replicated charts removal
               
/*
                    select top 100 
                            count(chart_id) over (partition by NPI__c,TIN_ID__c,FULL_ADDRESS,Phone_Number,Location__c) byNpiTInAddPhone_CountOfCharts_atLoc,
                                  #rel_qap_data.* from #rel_qap_data
                        where NPI__c is not null
                 and NPI__c = '1023088390'
                           and TIN_ID__c ='223693606'
                           and FULL_ADDRESS = '239 HURFVILLE-CROSS KEYS ROAD SUITE 250, SEWELL, NJ 08080  19107 | PHILADELPHIA | PA | 19107'
                           and Phone_Number ='8562628300'
                           and rownum_remove_chartLvl_reptions_for_cl_processing=1
                  order by NPI__c,TIN_ID__c,FULL_ADDRESS,Phone_Number,Location__c

    */        
                      
                                  

/* Locations:
  select count(1), COUNT(DISTINCT location__c)              from #rel_qap_data where rownum_remove_location_repeatition_forMasterProcessing=1 --47791
  select location__c, count(1) from #rel_qap_data group by location__c                                           --48998
  select location__c, count(1) from #rel_qap_data where rownum_remove_location_repeatition_forMasterProcessing=1 
                                               group by location__c having count(1) > 1  --0
   Location,Providers:
--select                          count(1) from #rel_qap_data where rownum_remove_chartLvl_reptions_for_cl_processing=1 --160081
  select location__c,Provider__c, count(1) from #rel_qap_data group by location__c,Provider__c                          --160081
  select location__c,Provider__c, count(1) from #rel_qap_data where rownum_remove_chartLvl_reptions_for_cl_processing=1 
                                       group by location__c,Provider__c having count(1) > 1 --0 */

                                                                 

--(C) Create data from Locations to be mastered and generate new_lid

              select Location2__c.id Location__c
                    ,convert(  varchar(32), HashBytes('MD5',
                      '#$%^&' + isnull(Location2__c.Provider_Full_Address__c,'') + '&^%$#'
                     +'#$%^&' + isnull(Location2__c.City__c,'') + '&^%$#'
                     +'#$%^&' + isnull(Location2__c.State__c,'') + '&^%$#'
                     +'#$%^&' + isnull(Location2__c.ZIP_Code__c,'') + '&^%$#'
                     +'#$%^&' + isnull(dbo.fn_cleanse_phone(isnull(Location2__c.Phone_1__c, Location2__c.Phone_2__c)),'') + '&^%$#')
                                , 2) NEW_LID_ALL
                    ,convert(  varchar(32), HashBytes('MD5',
                      '#$%^&' + isnull(Location2__c.Provider_Full_Address__c,'') + '&^%$#'
                     +'#$%^&' + isnull(Location2__c.City__c,'') + '&^%$#'
                     +'#$%^&' + isnull(Location2__c.State__c,'') + '&^%$#'
                     +'#$%^&' + isnull(Location2__c.ZIP_Code__c,'') + '&^%$#')
                                , 2) NEW_LID_ADD
                    ,convert(  varchar(32), HashBytes('MD5',
                      '#$%^&' + isnull(Location2__c.Provider_Full_Address__c,'') + '&^%$#'
                     +'#$%^&' + isnull(Location2__c.City__c,'') + '&^%$#'
                     +'#$%^&' + isnull(Location2__c.State__c,'') + '&^%$#'
                     +'#$%^&' + isnull(Location2__c.ZIP_Code__c,'') + '&^%$#'
                     +'#$%^&' + substring(isnull(dbo.fn_cleanse_phone(isnull(Location2__c.Phone_1__c, Location2__c.Phone_2__c)),'') ,1,9) + '&^%$#')
                                , 2) NEW_LID_ADD_BASE_PHONE
                                  ,substring(isnull(dbo.fn_cleanse_phone(isnull(Location2__c.Phone_1__c, Location2__c.Phone_2__c)),'') ,1,9) base_phone
                                  ,convert(  varchar(32), HashBytes('MD5',
                      '#$%^&' + isnull(dbo.fn_cleanse_phone(isnull(Location2__c.Phone_1__c, Location2__c.Phone_2__c)),'') + '&^%$#')
                                , 2) NEW_LID_PHONE
                    ,Location2__c.name SFDC_LID
                    ,Location2__c.Provider_Full_Address__c
                    ,Location2__c.City__c
                    ,Location2__c.State__c
                    ,Location2__c.ZIP_Code__c
                    ,dbo.fn_cleanse_phone(Location2__c.Phone_1__c) Phone_1__c
                    ,Location2__c.Phone_1_Ext__c
                    ,dbo.fn_cleanse_phone(Location2__c.Phone_2__c) Phone_2__c
                    ,Location2__c.Phone_2_Ext__c
                    ,Location2__c.Contact_Person__c
                    ,Location2__c.Contact_Person_2__c
                    ,#rel_qap_data.charts_at_loc 
                    ,Location2__c.Practice_Name__c
                    ,Location2__c.Client_Name__c
                    ,Location2__c.Fax_Number__c
                    ,Location2__c.Fax_Number_Console__c
                    ,Location2__c.Fax_Number_For_Display__c                
                    ,Location2__c.Record_Storage_Type__c
                    ,Location2__c.EMR_System__c
                    ,Location2__c.EMR_Version__c
                    ,Location2__c.Other_EMR_System__c
                    ,Location2__c.Num_of_Charts__c
                    ,Location2__c.Num_of_Charts_Recovered__c
                    ,Location2__c.Charts_Remaining__c
                    ,Location2__c.Num_of_Charts_Scheduled__c
                    ,Location2__c.of_charts_REL__c
                    ,Location2__c.Percent_of_Charts_Recovered__c
                    ,Location2__c.Appointment_Type__c
                    ,Location2__c.Workflow_Status__c
                    ,Location2__c.Invoices__c
                    ,Location2__c.PNP_Reason__c 
                    ,Location2__c.Location__Latitude__s
                    ,Location2__c.Location__Longitude__s             
                into #poc__location_master_hash_gen
                from #rel_qap_data 
                join Location2__c
                  on #rel_qap_data.location__c = location2__c.id
                 and rownum_remove_location_repeatition_forMasterProcessing=1
               where (Location2__c.Provider_Full_Address__c is not null 
                      or isnull(Location2__c.Phone_1__c,Location2__c.Phone_2__c) is not null)
            order by Provider_Full_Address__c
                    ,Phone_2__c  
            --(48902 row(s) affected) Excluding address & ph nulls
            --(47707 row(s) affected) Post replicated charts removal
                
--select COUNT(1), COUNT(DISTINCT lOCATION__c), COUNT(DISTINCT NEW_LID) from #poc__location_master_hash_gen 
          

--select * from #poc__location_master_hash_gen order by NEW_LID_ADD_BASE_PHONE, Provider_Full_Address__c

--(D) SFLID-NEWLID_MAP (THE MERGE REPORT!!!!!!)
if object_id('tempdb..##poc__lm__sfdcLid_newLid_map') is not null drop table ##poc__lm__sfdcLid_newLid_map
----NEW_LID_ALL
--              select New_lid_all, Location__c
--                    ,SFDC_LID,count(SFDC_LID) over(partition by New_lid_all) as count_sfLid_at_NewLIdAll 
--                into #poc__lm__sfdcLid_newLidAll_map  
--                from #poc__location_master_hash_gen 
--            --(47707 row(s) affected)
--
----NEW_LID_ADD
--              select NEW_LID_ADD, Location__c
--                    ,SFDC_LID,count(SFDC_LID) over(partition by NEW_LID_ADD) as count_sfLid_at_NewLIdAdd
--                into #poc__lm__sfdcLid_newLidAdd_map  
--                from #poc__location_master_hash_gen 
--
----NEW_LID_PHONE
--              select NEW_LID_PHONE, Location__c
--                    ,SFDC_LID,count(SFDC_LID) over(partition by NEW_LID_PHONE) as count_sfLid_at_NewLIdPhone
--                into #poc__lm__sfdcLid_newLidPhone_map  
--                from #poc__location_master_hash_gen 
--
----NEW_LID_ADD_BASE_PHONE
--              select NEW_LID_ADD_BASE_PHONE, Location__c
--                    ,SFDC_LID,count(SFDC_LID) over(partition by NEW_LID_ADD_BASE_PHONE) as count_sfLid_at_NewLIdBasePhone
--                into #poc__lm__sfdcLid_newLidBasePhone_map  
--                from #poc__location_master_hash_gen 


--(D.NEW) SFLID-NEWLID_MAP (THE MERGE REPORT!!!!!!)
if object_id('tempdb..##poc__lm__sfdcLid_newLid_map') is not null drop table ##poc__lm__sfdcLid_newLid_map
              select New_lid_all
                    ,NEW_LID_ADD
                    ,NEW_LID_PHONE
                    ,NEW_LID_ADD_BASE_PHONE
                    ,Location__c
                    ,SFDC_LID
                    ,count(SFDC_LID) over(partition by New_lid_all) as count_sfLid_at_NewLIdAll 
                    ,count(SFDC_LID) over(partition by NEW_LID_ADD) as count_sfLid_at_NewLIdAdd
                    ,count(SFDC_LID) over(partition by NEW_LID_PHONE) as count_sfLid_at_NewLIdPhone
                    ,count(SFDC_LID) over(partition by NEW_LID_ADD_BASE_PHONE) as count_sfLid_at_NewLIdBasePhone
                into ##poc__lm__sfdcLid_newLid_map  
                from #poc__location_master_hash_gen 
            order by count(SFDC_LID) over(partition by New_lid_all) desc
                     --count(SFDC_LID) over(partition by NEW_LID_PHONE) desc

/*
select count(1) from ##poc__lm__sfdcLid_newLid_map 
        where count_sfLid_at_NewLIdBasePhone=3
     order by New_lid_add_base_phone


select 'NEW_LID_ALL', COUNT(1), COUNT(DISTINCT lOCATION__c), COUNT(DISTINCT NEW_LID_ALL) from #poc__lm__sfdcLid_newLidAll_map
union all
select 'NEW_LID_ADD', COUNT(1), COUNT(DISTINCT lOCATION__c), COUNT(DISTINCT NEW_LID_ADD) from #poc__lm__sfdcLid_newLidAdd_map
union all
select 'NEW_LID_PHONE',COUNT(1), COUNT(DISTINCT lOCATION__c), COUNT(DISTINCT NEW_LID_PHONE) from #poc__lm__sfdcLid_newLidPhone_map
union all
select 'NEW_LID_ADD_BASE_PHONE', COUNT(1), COUNT(DISTINCT lOCATION__c), COUNT(DISTINCT NEW_LID_ADD_BASE_PHONE) from #poc__lm__sfdcLid_newLidBasePhone_map
order by 4 desc
*/


/*
select COUNT(1), COUNT(DISTINCT lOCATION__c), COUNT(DISTINCT NEW_LID) from poc__lm__sfdcLid_newLid_map --48902  48194
                                                                     where count_sfLid_at_NewLId > 1 order by 3  --1243, 535
                                                                                                                                                                  --1185, 510
select NEW_LID, COUNT(1), COUNT(DISTINCT lOCATION__c), COUNT(DISTINCT SFDC_LID) from poc__lm__sfdcLid_newLid_map 
                                                                               where count_sfLid_at_NewLId > 1 GROUP BY NEW_LID --535
                                                                                                                                                                                      --510
select * from poc_lid_merge_reference ORDER BY 3

select 48902-48194  --708 
select 47707-47032  --675 fewer lids generated  

--*****************************i.e. 708 fewer LIDs after simple merging, this shall further get reduced after data standerdization followed by deduping************************
*/



--(F) LOCATION MASTERING
if object_id('tempdb..#poc__lm__locations_master') is not null drop table #poc__lm__locations_master
go    
       with buildMaster as (
              select count(SFDC_LID) 
                              over(partition by NEW_LID_ALL) 
                     as count_sfLid_at_NewLId
                    ,row_number()    
                                   over(partition by NEW_LID_ALL 
                              order by charts_at_loc desc) 
                     as masteringFilter
                    ,#poc__location_master_hash_gen.* 
                from #poc__location_master_hash_gen) --ORDER BY 1 DESC
              select masteringFilter,
                     NEW_LID_ALL new_lid
                    ,Location__c
                    ,SFDC_LID
                    ,Provider_Full_Address__c
                    ,City__c
                    ,State__c
                    ,ZIP_Code__c
                    ,Phone_1__c
                    ,Phone_1_Ext__c
                    ,Phone_2__c
                    ,Phone_2_Ext__c
                    ,Contact_Person__c
                    ,Contact_Person_2__c
                    ,Practice_Name__c
                    ,Client_Name__c
                    ,Fax_Number__c
                    ,Fax_Number_Console__c
                    ,Fax_Number_For_Display__c
                    ,Record_Storage_Type__c
                    ,EMR_System__c
                    ,EMR_Version__c
                    ,Other_EMR_System__c
                    ,Num_of_Charts__c
                    ,charts_at_loc as Num_of_Charts_actual_retrievals_at_loc 
                    ,100*(charts_at_loc/Num_of_Charts__c) Percent_of_Charts_Recovered__c
                    ,Appointment_Type__c
                    ,Workflow_Status__c
                    ,Invoices__c
                    ,PNP_Reason__c
                    ,Location__Latitude__s
                    ,Location__Longitude__s
                    ,count_sfLid_at_NewLId
                into #poc__lm__locations_master
                from buildMaster --48902
               where masteringFilter = 1 
            order by NEW_LID_ALL
                    ,Provider_Full_Address__c
                    ,isnull(Phone_1__c,Phone_2__c)
  --48194
  --(47032 row(s) affected) Post removal of replicated charts
  --(46935 row(s) affected) Post phone number standardization
    
--select * from #poc__lm__locations_master__newLidAll

--(G) CL_NEW-LID_MAP
if object_id('tempdb..#poc__lm__chaseList_newLid_map') is not null drop table #poc__lm__chaseList_newLid_map
--go
--    select distinct
--           poc__lm__sfdcLid_newLid_map.New_lid
--        --,poc__lm__sfdcLid_newLid_map.Location__c
--        --,poc__lm__sfdcLid_newLid_map.sfdc_lid
--        --,#rel_qap_data.provider__c
--          ,provider__c.NPI__c          
--          ,provider__c.TIN_ID__c
--          ,provider__c.Provider_Full_Address_With_Suite__c + ' | ' + provider__c.City__c  + ' | ' + State__c  + ' | '  + ZIP_Code__c AS FULL_ADDRESS
--          ,isnull(provider__c.Phone_Number__c,Secondary_Phone_Number__c) Phone_Number__c
--      into #poc__lm__chaseList_newLid_map
--      from ##poc__lm__sfdcLid_newLid_map 
--      join #rel_qap_data    
--        on poc__lm__sfdcLid_newLid_map.Location__c = #rel_qap_data.Location__c 
--       and rownum_remove_chartLvl_reptions_for_cl_processing=1
--      join provider__c
--        on #rel_qap_data.provider__c = provider__c.id
--    order by --count_sfLid_at_NewLId,
--               New_lid desc
----(144698 row(s) affected)  
--
--
--select * from ##poc__lm__sfdcLid_newLid_map 
--        where count_sfLid_at_NewLIdBasePhone=3
--     order by New_lid_add_base_phone



    select 
           ##poc__lm__sfdcLid_newLid_map.New_lid_all
        --,##poc__lm__sfdcLid_newLid_map.Location__c
        --,##poc__lm__sfdcLid_newLid_map.sfdc_lid
        --,#rel_qap_data.provider__c
          ,#rel_qap_data.NPI__c          
          ,#rel_qap_data.TIN_ID__c
          ,#rel_qap_data.FULL_ADDRESS
          ,#rel_qap_data.Phone_Number__c
          ,sum(#rel_qap_data.charts_at_loc                                        ) charts_at_loc
          ,sum(#rel_qap_data.CountOfCharts_atLoc_byNpiTinAddPhone) CountOfCharts_atLoc_byNpiTinAddPhone
          ,sum(#rel_qap_data.CountOfCharts_atLoc_byNpiAddPhone       ) CountOfCharts_atLoc_byNpiAddPhone
          ,sum(#rel_qap_data.CountOfCharts_atLoc_byNpiPhone          ) CountOfCharts_atLoc_byNpiPhone
          ,sum(#rel_qap_data.CountOfCharts_atLoc_byNpiAdd            ) CountOfCharts_atLoc_byNpiAdd
          ,sum(#rel_qap_data.CountOfCharts_atLoc_byTinAddPhone       ) CountOfCharts_atLoc_byTinAddPhone
          ,sum(#rel_qap_data.CountOfCharts_atLoc_byTinPhone          ) CountOfCharts_atLoc_byTinPhone
          ,sum(#rel_qap_data.CountOfCharts_atLoc_byTinAdd            ) CountOfCharts_atLoc_byTinAdd
          ,sum(#rel_qap_data.CountOfCharts_atLoc_byPhone             ) CountOfCharts_atLoc_byPhone
          ,sum(#rel_qap_data.CountOfCharts_atLoc_byAddress       ) CountOfCharts_atLoc_byAddress         
    --  into #poc__lm__chaseList_newLid_map
      from ##poc__lm__sfdcLid_newLid_map 
      join #rel_qap_data    
        on ##poc__lm__sfdcLid_newLid_map.Location__c = #rel_qap_data.Location__c 
       and rownum_remove_chartLvl_reptions_for_cl_processing=1
  group by ##poc__lm__sfdcLid_newLid_map.New_lid_all
          ,#rel_qap_data.NPI__c          
          ,#rel_qap_data.TIN_ID__c
          ,#rel_qap_data.FULL_ADDRESS
          ,#rel_qap_data.Phone_Number__c
  order by --count_sfLid_at_NewLId,
               New_lid_all desc
                        --144751
                        --144679
