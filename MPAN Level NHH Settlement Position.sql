-- Databricks notebook source
CREATE OR REPLACE TABLE analysts.idm_and_settlement.GSP AS
WITH settlement_runs AS (
  SELECT 
    MAX(`DA Run Date`) -1 AS Run_Date,
    MAX(`date`) AS Max_Settlement_Date,
    COALESCE(LAG(MAX(`date`)) OVER (ORDER BY MAX(`date`)) + INTERVAL 1 DAY, DATE '1900-01-01') AS Min_Settlement_Date,
    run
  FROM analysts.idm_and_settlement.settlement_runs
  WHERE `DA Run Date` <= CURRENT_DATE() +1
  GROUP BY run
),

rf_settlement AS (
  SELECT MAX(Max_Settlement_Date) AS RF_Max_Date
  FROM settlement_runs
  WHERE run = 'RF'
),

d0004_history AS (
SELECT DISTINCT 
    zhv.ID,
    J1064 AS File_Identifier,
    J0934 AS From_Market_Participant_Role_Code,
    J0938 AS From_Market_Participant_Id,
    J0280 AS File_Creation_Timestamp,
    J0003 AS MPAN,
    J0171 AS Reading_Type,
    J0016 AS Reading_Date_and_Time,
    J0024 AS SVCC,
    svcc.Description AS SVCC_Description,
    J0012 AS Additional_Information,
    CASE WHEN J0024 in ('19','20','27') THEN 'Non-Auditable' ELSE 'Auditable' END AS Audit_Status
  FROM source_systems.dataflows.d0004_zhv zhv
  INNER JOIN source_systems.dataflows.d0004_014 _014
    ON zhv.ID = _014.ZHV_FK
  INNER JOIN source_systems.dataflows.d0004_015 _015
    ON _014.ID = _015.014_FK
  INNER JOIN source_systems.dataflows.d0004_016 _016
    ON _015.ID = _016.015_FK
  INNER JOIN analysts.idm_and_settlement.svcc_descriptions svcc
    ON _016.J0024 = svcc.Value
),

d0004_counts AS (
  SELECT 
    m.j0003 AS MPAN,
    m.j0049 AS supply_start_date,
    m.j0117 AS supply_end_date,
    COUNT(d.*) AS d0004_count
  FROM source_systems.afms_elec_evo_dbo.mpan m 
  LEFT JOIN d0004_history d
    ON m.j0003 = d.MPAN
    AND d.Reading_Date_and_Time BETWEEN m.j0049 AND COALESCE(m.j0117, CURRENT_DATE)
    AND NOT (
      d.SVCC = '20' AND (
        lower(d.additional_information) LIKE '%comms%' OR
        lower(d.additional_information) LIKE '%fault%' OR
        lower(d.additional_information) LIKE '%d+%' OR
        lower(d.additional_information) LIKE '%remote%'
      )
    )
  GROUP BY m.j0003, m.j0049, m.j0117
),

d0004_since_aah_counts AS (
  SELECT 
    m.j0003 AS mpan,
    m.j0049 AS supply_start_date,
    m.j0117 AS supply_end_date,
    COUNT(d.MPAN) AS d0004_count_since_aah
  FROM source_systems.afms_elec_evo_dbo.mpan m

  LEFT JOIN (
    SELECT mpan, MAX(etd_aa) AS etd_aa
    FROM reporting.industry_dataflows_derived.d0019_from_d0011_confirmed_dc
    GROUP BY mpan
  ) e ON e.mpan = m.j0003

  LEFT JOIN d0004_history d
    ON d.MPAN = m.j0003
    AND d.Reading_Date_and_Time BETWEEN 
        CASE 
          WHEN e.etd_aa IS NULL OR e.etd_aa < m.j0049 THEN m.j0049
          ELSE e.etd_aa
        END
        AND COALESCE(m.j0117, CURRENT_DATE)
    AND NOT (
      d.SVCC = '20' AND (
        lower(d.additional_information) LIKE '%comms%' OR
        lower(d.additional_information) LIKE '%fault%' OR
        lower(d.additional_information) LIKE '%d+%' OR
        lower(d.additional_information) LIKE '%remote%'
      )
    )
  GROUP BY m.j0003, m.j0049, m.j0117
),

latest_d0004 AS (
  SELECT 
    d.MPAN,
    d.Reading_Date_and_Time,
    d.SVCC,
    d.SVCC_Description,
    d.additional_information,
    d.Audit_Status,
    ROW_NUMBER() OVER (PARTITION BY d.MPAN, m.j0049, m.j0117 ORDER BY d.Reading_Date_and_Time DESC) AS rn,
    m.j0049 AS supply_start_date,
    m.j0117 AS supply_end_date
  FROM d0004_history d
  INNER JOIN source_systems.afms_elec_evo_dbo.mpan m
    ON d.MPAN = m.j0003
    AND d.Reading_Date_and_Time BETWEEN m.j0049 AND COALESCE(m.j0117, CURRENT_DATE)
    AND NOT (
      d.SVCC = '20' AND (
        lower(d.additional_information) LIKE '%comms%' OR
        lower(d.additional_information) LIKE '%fault%' OR
        lower(d.additional_information) LIKE '%d+%' OR
        lower(d.additional_information) LIKE '%remote%'
      )
    )
),

customer_data AS (
  SELECT
    mpan,
    cu.customer_type,
    cu.customer_name,
    cu.customer_number,
    ROW_NUMBER() OVER (PARTITION BY mpan ORDER BY ms.from_date DESC) AS rn
  FROM refined.customer.meter_point_supply_period_elec ms
  LEFT JOIN refined.customer.agreements ag
    ON ag.meterpoint_fk = ms.meterpoint_fk
    AND ag.meterpoint_is_latest = TRUE
    AND ag.flags.cancelled_flag = FALSE
  LEFT JOIN refined.customer.account ac
    ON ac.account_pk = ag.account_fk
  LEFT JOIN refined.customer.customer cu
    ON cu.customer_pk = ac.customer_fk
),

gsp_agents AS (
  SELECT * FROM VALUES
    ('_A', 'Aaron'),
    ('_B', 'Bridie'),
    ('_C', 'Myllie'),
    ('_D', 'Dee'),
    ('_E', 'Irene'),
    ('_F', 'Lynn'),
    ('_G', 'Lynn'),
    ('_H', 'Scott'),
    ('_J', 'Dee'),
    ('_K', 'Lottie'),
    ('_L', 'Lindsay'),
    ('_M', 'Lottie'),
    ('_N', 'Scott'),
    ('_P', 'Bridie')
  AS t (gsp, agent)
),

address_data AS (
  SELECT DISTINCT
    j0003 AS mpan,
    PK,
    TRIM(
      CONCAT_WS(', ',
        NULLIF(TRIM(J1036), ''),
        NULLIF(TRIM(J1037), ''),
        NULLIF(TRIM(J1038), ''),
        NULLIF(TRIM(J1039), ''),
        NULLIF(TRIM(J1040), ''),
        NULLIF(TRIM(J1041), ''),
        NULLIF(TRIM(J1042), ''),
        NULLIF(TRIM(J1043), ''),
        NULLIF(TRIM(J1044), '')
      )
    ) AS Address,
    J0263 AS Postcode
  FROM source_systems.afms_elec_evo_dbo.mpan
),

affecting_flags AS (
  SELECT 
    m.PK,
    m.j0003 AS mpan,
    m.j0049 AS supply_start_date,
    m.j0117 AS supply_end_date,
    m.j0473 AS disconnection_date,
    m.j0080 AS energisation_status,
    m.J0297 AS deenergisation_date,
    e.etd_aa,
    CASE
      WHEN m.j0049 > (SELECT Max_Settlement_Date FROM settlement_runs WHERE run = 'II') THEN FALSE -- supply start date not affecting until past II
      WHEN m.j0117 < (SELECT Max_Settlement_Date FROM settlement_runs WHERE run = 'RF') THEN FALSE -- supply end date not affecting beyond RF
      WHEN m.j0473 < (SELECT Max_Settlement_Date FROM settlement_runs WHERE run = 'RF') THEN FALSE -- disconnection date not affecting beyond RF 
      WHEN (m.j0080 = 'D' AND m.J0297 < (SELECT Max_Settlement_Date FROM settlement_runs WHERE run = 'RF')) THEN FALSE -- de-energisation is beyond RF
      WHEN m.j0049 >= m.j0117 THEN FALSE -- SSD >= SED - Withdrawn registrations
      WHEN m.j0049 >= m.j0473 THEN FALSE -- SSD >= disconnection date, disconnected before joined us
      WHEN e.etd_aa >= m.j0117 THEN FALSE  -- Latest read >= SED 
      WHEN e.etd_aa >= m.j0473 THEN FALSE -- disconnection date >= SED read up to disconnection date
      WHEN (m.j0080 = 'D' AND e.etd_aa >= m.J0297) THEN FALSE --supply deenergised but read up to deenergisation date
      WHEN (m.j0080 = 'D' AND m.J0297 <= m.j0049) THEN FALSE --when its been deenergised before its come on supply 
      ELSE TRUE 
    END AS affecting_settlement
  FROM source_systems.afms_elec_evo_dbo.mpan m
  LEFT JOIN (
    SELECT 
      mpan, 
      MAX(etd_aa) AS etd_aa,
      ROW_NUMBER() OVER (PARTITION BY mpan ORDER BY MAX(flow_creation_ts) DESC) AS row_num 
    FROM reporting.industry_dataflows_derived.d0019_from_d0011_confirmed_dc
    GROUP BY mpan
  ) e 
    ON m.j0003 = e.mpan AND e.row_num = 1
)

SELECT 
  m.PK AS ID,
  m.j0003 AS mpan,
  COALESCE(
    CASE WHEN e.etd_aa IS NULL OR e.etd_aa < m.j0049 THEN m.j0049 ELSE e.etd_aa END,
    m.j0049
  ) AS Last_Valid_Read_Date,
  m.j0049 AS supply_start_date, 
  m.j0117 AS supply_end_date,
  ROUND(ANY_VALUE(CASE WHEN d.eac IS NULL THEN de.Researched_Average_EAC ELSE d.eac END), 2) AS eac,
  m.j0071 AS profile_class,
  m.j0076 AS ssc,
  m.j0066 AS GSP,
  ga.agent AS Agent_Assigned,
  sr.run,
  af.affecting_settlement,
  LLF.Export_indicator,
  CASE WHEN m.j0117 IS NULL OR m.j0117 >= CURRENT_DATE THEN TRUE ELSE FALSE END AS Current_Supplier,

  ld.Reading_Date_and_Time AS D0004_Date,
  ld.SVCC,
  ld.SVCC_Description,
  ld.additional_information,
  ld.Audit_Status,

  dc.d0004_count AS D0004_Count_Since_SSD,
  dcs.d0004_count_since_aah AS D0004_Count_Since_AAH,

  cd.customer_type,
  cd.customer_name,
  cd.customer_number,

  ad.Address,
  ad.Postcode,

  m.J1833 AS DCC_Service_Flag,

  lm.j0483 AS meter_type,

RANK() OVER (
    PARTITION BY ga.agent  -- Rank MPANs separately for each agent
    ORDER BY 
      CASE
        WHEN af.affecting_settlement IS TRUE THEN  -- Only rank if the MPAN affects settlement
          CASE 
            WHEN sr.run IN ('RF') THEN 
              -- RF : High priority, multiply EAC by 5
              ROUND(ANY_VALUE(CASE WHEN d.eac IS NULL THEN de.Researched_Average_EAC ELSE d.eac END), 2) * 5

            WHEN sr.run IN ('R3') THEN 
              -- R3: Scale EAC by % of time since last read (or SSD) relative to time between now and RF max
              ROUND(ANY_VALUE(CASE WHEN d.eac IS NULL THEN de.Researched_Average_EAC ELSE d.eac END), 2) * 
              (DATEDIFF(CURRENT_DATE, COALESCE(e.etd_aa, m.j0049)) / 
              NULLIF(DATEDIFF(CURRENT_DATE, ANY_VALUE(rs.RF_Max_Date)), 0))

            WHEN sr.run IN ('R1', 'R2', 'SF') THEN 
              -- R1, R2, SF: Same as R3, but weighted at 50%
              ROUND(ANY_VALUE(CASE WHEN d.eac IS NULL THEN de.Researched_Average_EAC ELSE d.eac END), 2) * 
              (DATEDIFF(CURRENT_DATE, COALESCE(e.etd_aa, m.j0049)) / 
              NULLIF(DATEDIFF(CURRENT_DATE, ANY_VALUE(rs.RF_Max_Date)), 0)) * 0.5

            WHEN sr.run = 'II' THEN 
              -- II: Same as R3, but weighted at 10% (lowest priority)
              ROUND(ANY_VALUE(CASE WHEN d.eac IS NULL THEN de.Researched_Average_EAC ELSE d.eac END), 2) * 
              (DATEDIFF(CURRENT_DATE, COALESCE(e.etd_aa, m.j0049)) / 
              NULLIF(DATEDIFF(CURRENT_DATE, ANY_VALUE(rs.RF_Max_Date)), 0)) * 0.1

            ELSE 0  -- Fallback if run type is unexpected
          END
        ELSE 0  -- If not affecting settlement, assign 0 priority
      END DESC  -- Sort by descending importance (higher scores ranked higher)
)
 AS Rank,
 ROW_NUMBER() OVER (PARTITION BY m.j0003 ORDER BY m.j0049 DESC) AS row_num
FROM source_systems.afms_elec_evo_dbo.mpan m

LEFT JOIN analysts.idm_and_settlement.latest_d0019_aggregated d
  ON d.mpan = m.j0003

LEFT JOIN (
  SELECT 
    mpan, 
    MAX(etd_aa) AS etd_aa,
    ROW_NUMBER() OVER (PARTITION BY mpan ORDER BY MAX(flow_creation_ts) DESC) AS row_num 
  FROM reporting.industry_dataflows_derived.d0019_from_d0011_confirmed_dc
  GROUP BY mpan
) e 
  ON m.j0003 = e.mpan AND e.row_num = 1

LEFT JOIN latest_d0004 ld
  ON ld.MPAN = m.j0003
  AND ld.supply_start_date = m.j0049
  AND (ld.supply_end_date = m.j0117 OR (ld.supply_end_date IS NULL AND m.j0117 IS NULL))
  AND ld.rn = 1

LEFT JOIN d0004_counts dc
  ON dc.mpan = m.j0003
  AND dc.supply_start_date = m.j0049
  AND (dc.supply_end_date = m.j0117 OR (dc.supply_end_date IS NULL AND m.j0117 IS NULL))

LEFT JOIN d0004_since_aah_counts dcs
  ON dcs.mpan = m.j0003
  AND dcs.supply_start_date = m.j0049
  AND (dcs.supply_end_date = m.j0117 OR (dcs.supply_end_date IS NULL AND m.j0117 IS NULL))

LEFT JOIN analysts.idm_and_settlement.non_refreshable_default_eacs_ de
  ON m.j0071 = de.Profile_Class_Id 
  AND try_cast(m.j0076 AS BIGINT) = de.Standard_Settlement_Configuration_Id 
  AND m.j0066 = de.GSP_Group_Id
  AND d.mpan IS NULL

LEFT JOIN settlement_runs sr
  ON COALESCE(
    CASE WHEN e.etd_aa IS NULL OR e.etd_aa < m.j0049 THEN m.j0049 ELSE e.etd_aa END,
    m.j0049
  ) BETWEEN sr.Min_Settlement_Date AND sr.Max_Settlement_Date

LEFT JOIN analysts.idm_and_settlement.import_export_indicator_llfc LLF
  ON m.DISTRIBUTOR = LLF.Market_Participant_ID 
  AND m.J0147 = LLF.Line_Loss_Factor_Class_ID

LEFT JOIN customer_data cd
  ON cd.mpan = m.j0003 
  AND cd.rn = 1

LEFT JOIN analysts.idm_and_settlement.latest_elec_meter lm
  ON m.PK = lm.MPAN_LNK

LEFT JOIN gsp_agents ga 
  ON m.j0066 = ga.gsp

LEFT JOIN address_data ad 
  ON ad.PK = m.PK

LEFT JOIN affecting_flags af
  ON af.PK = m.PK 
  AND af.mpan = m.j0003 
  AND af.supply_start_date = m.j0049 
  AND (af.supply_end_date = m.j0117 OR (af.supply_end_date IS NULL AND m.j0117 IS NULL))

LEFT JOIN rf_settlement rs ON TRUE


WHERE m.J0082 IN ('A', 'F')
AND (m.j0049 < m.J0117 OR m.J0117 IS NULL)

GROUP BY 
  m.PK, m.j0003, e.etd_aa, m.j0071, m.j0076, m.j0066, ga.agent, m.j0473, m.j0080, m.j0297, sr.run, LLF.Export_indicator, 
  m.j0049, m.j0117, ld.Reading_Date_and_Time, ld.SVCC, ld.SVCC_Description, af.affecting_settlement, ld.additional_information, ld.Audit_Status,
  dc.d0004_count, dcs.d0004_count_since_aah, cd.customer_type, cd.customer_name, cd.customer_number,
  ad.Address,  ad.Postcode, d.eac, 
  m.J1833, lm.j0483

ORDER BY m.j0003, m.j0049 DESC
;
SELECT * FROM analysts.idm_and_settlement.GSP
where customer_name ='Savills'
and meter_type = 'N'
and customer_type = 'Commercial'
and affecting_settlement = true
and export_indicator = false
and run <> 'RF'
and supply_start_date <= DATE '2024-07-07'
and (supply_end_date >= DATE '2024-07-07' or supply_end_date is null)
