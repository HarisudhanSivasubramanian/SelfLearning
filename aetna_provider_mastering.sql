--Project : Aetna|2015 Commercial Phase 1|MRR|03150819
WITH project_selection
AS (SELECT
          Client_Name__c
        , project_name__c
        , COUNT(1) count_recs
FROM
          Provider__c
WHERE
          1                   = 1
          AND project_name__c = 'Aetna|2015 Commercial Phase 1|MRR|03150819'
GROUP BY
          Client_Name__c
        , project_name__c) -- 27436 rows affected

--From PL-Jn, collect provider and location information   
,
provider_loc_reference
AS (SELECT
          ProviderLocationJunction__c.provider__c
        , ProviderLocationJunction__c.location__c
        , ROW_NUMBER() OVER (
                   PARTITION BY ProviderLocationJunction__c.location__c
                           , ProviderLocationJunction__c.Provider__c
                   ORDER BY
                             ProviderLocationJunction__c.LastModifiedDate) AS rownum_remove_lid_dupes_for_a_provider
        , CASE
                    WHEN ISNULL(ProviderLocationJunction__c.LastModifiedDate, ProviderLocationJunction__c.CreatedDate) > ProviderLocationJunction__c.CreatedDate
                    THEN ProviderLocationJunction__c.LastModifiedDate
                    ELSE ProviderLocationJunction__c.CreatedDate
          END AS plj_record_date
FROM
          project_selection
          JOIN
                    ProviderLocationJunction__c
          ON
                    project_selection.project_name__c = ProviderLocationJunction__c.project_name__c) 
					
					--(27485) row(s) affected) 
					--select * from provider_loc_reference where rownum_remove_lid_dupes_for_a_provider=1 
, rel_qap_data AS
(
          SELECT
                    provider_loc_reference.Location__c
                  , provider_loc_reference.Provider__c
                  , provider_loc_reference.plj_record_date
                  , ROW_NUMBER() OVER (
                             PARTITION BY provider_loc_reference.location__c
                             ORDER BY
                                       plj_record_date) AS rownum_remove_location_repeatition_forMasterProcessing
                  , ROW_NUMBER() OVER (
                             PARTITION BY provider_loc_reference.location__c
                                     , provider_loc_reference.Provider__c
                             ORDER BY
                                       CASE
                                                 WHEN ISNULL(Chart__c.LastModifiedDate, Chart__c.CreatedDate) > Chart__c.CreatedDate
                                                 THEN Chart__c.LastModifiedDate
                                                 ELSE Chart__c.CreatedDate
                                       END DESC) AS rownum_remove_chartLvl_reptions_for_cl_processing
                  , COUNT(provider_loc_reference.Provider__c) OVER (
                                                          PARTITION BY provider_loc_reference.location__c) AS provider_charts_at_loc
                  , Chart__c.Appointment_Type__c
                  , COUNT(Chart__c.id) OVER (
                                   PARTITION BY Chart__c.Location__c
                                           , Appointment_Type__c) AS countOfChartsAtLocationbyAppointmentType
          FROM
                    provider_loc_reference
                    JOIN
                              Chart__c
                    ON
                              (
                                        provider_loc_reference.Location__c     = Chart__c.Location__c
                                        AND provider_loc_reference.Provider__c = Chart__c.Provider__c
                              )
                              AND FileBound_Disposition_Code__c                                 = 'QAP'
                              AND FileBound_Status__c                                           = 'REL'
                              AND MRT_Name__c                                                  <> 'Replication'
                              AND provider_loc_reference.rownum_remove_lid_dupes_for_a_provider = 1
)
--select * from rel_qap_data --242795 rows affecred