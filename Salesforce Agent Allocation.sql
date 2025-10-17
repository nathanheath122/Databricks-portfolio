-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Agent Table

-- COMMAND ----------

-- Drop the table if it already exists
DROP TABLE IF EXISTS analysts.idm_and_settlement.Salesforce_Agents;

-- Create the table
CREATE TABLE IF NOT EXISTS analysts.idm_and_settlement.Salesforce_Agents (
    Agent STRING, 
    adjusted_team STRING,
    open_case_count INT,  -- Count of open cases by Agent and adjusted_team
    Hours_Worked FLOAT    -- Working hours for the agent
);

-- Insert distinct Agent and adjusted_team combinations with open case count and working hours,
-- filtering out cases where no agent is assigned.
INSERT INTO analysts.idm_and_settlement.Salesforce_Agents (Agent, adjusted_team, open_case_count, Hours_Worked)
SELECT 
    Owner AS Agent, 
    CASE 
        WHEN UPPER(SUBJECT) LIKE '%COMPLEX%' THEN 'Complex Cases'
        ELSE Team
    END AS adjusted_team,
    COUNT(*) AS open_case_count,
    CASE
        WHEN Owner = 'Paul Twinning' THEN 36.5
        WHEN Owner = 'Sam Robinthwaite' THEN 36.5
        WHEN Owner = 'Anna Haigh' THEN 36.5
        WHEN Owner = 'Lynn Symonds' THEN 22.5
        --WHEN Owner = 'Irene Peters' THEN 22.5
        WHEN Owner = 'Scott Underhill' THEN 36.5
        WHEN Owner = 'Simon Ellis' THEN 36.5
        WHEN Owner = 'Aaron Wilkins' THEN 36.5
        WHEN Owner = 'Ffion Morgan' THEN 36.5
        WHEN Owner = 'Lindsay Lines' THEN 30
        WHEN Owner = 'Myriam Richards' THEN 36.5
        WHEN Owner = 'Myllie Kitchen' THEN 36.5
        WHEN Owner = 'Hannah Dingle' THEN 15
        WHEN Owner = 'Lottie Aldridge' THEN 22.5
        WHEN Owner = 'Mark Askew' THEN 36.5
        WHEN Owner = 'Hannah Kimber' THEN 24
        WHEN Owner = 'Pippa Long' THEN 15
        WHEN Owner = 'Andi Toman' THEN 36.5
        WHEN Owner = 'Tony Pilling' THEN 36.5
        WHEN Owner = 'Jade Gough' THEN 36.5
        WHEN Owner = 'Josh Hatton' THEN 36.5
        WHEN Owner = 'Charlie Iles' THEN 36.5
        WHEN Owner = 'Simon Bonello' THEN 30
        WHEN Owner = 'Ella McCulla' THEN 36.5
        WHEN Owner = 'Katie Farley' THEN 36.5
        WHEN Owner = 'Greg Warman' THEN 36.5
        WHEN Owner = 'Adam Sinclair' THEN 36.5
        WHEN Owner = 'Bridie Payne' THEN 22.5
        WHEN Owner = 'Lucy Richards' THEN 36.5
        WHEN Owner = 'Blue Burns' THEN 36.5 
        WHEN Owner = 'Dee Woodley' THEN 36.5 
       -- WHEN Owner = 'Stephanie Billivant' THEN 36.5
        ELSE 0  -- Default value for agents not in the list
    END AS Hours_Worked
FROM 
    analysts.idm_and_settlement.salesforce_all_cases
WHERE 
    STATUS <> 'Closed'                      -- Only count open cases
    AND Case_Relationship IN ('LoneCase', 'ChildCase')  -- Only LoneCase and ChildCase
    AND Owner IS NOT NULL                   -- Exclude cases with no agent assigned
GROUP BY 
    Owner,
    CASE 
        WHEN UPPER(SUBJECT) LIKE '%COMPLEX%' THEN 'Complex Cases'
        ELSE Team
    END;

-- Verify the data
SELECT * FROM analysts.idm_and_settlement.Salesforce_Agents;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## No agent assigned table 

-- COMMAND ----------

-- Create the table
CREATE TABLE IF NOT EXISTS analysts.idm_and_settlement.SalesforceNoAgentCases AS
SELECT 
    COALESCE(Owner, 'No Agent Assigned') AS Altered_Agent,  -- Adjusted agent column
    CASE 
        WHEN UPPER(SUBJECT) LIKE '%COMPLEX%' THEN 'Complex Cases'
        ELSE Team
    END AS adjusted_team,  -- Adjusted team column
    *  -- Keep all other columns if needed
FROM 
    analysts.idm_and_settlement.salesforce_all_cases
WHERE 
    STATUS <> 'Closed'  -- Only open cases
    AND COALESCE(Owner, 'No Agent Assigned') = 'No Agent Assigned'  -- Ensuring no agent assigned
    AND CASE 
            WHEN UPPER(SUBJECT) LIKE '%COMPLEX%' THEN 'Complex Cases'
            ELSE Team
        END != 'Complex Cases'  -- Exclude "Complex Cases" team
    AND Case_Relationship IN ('LoneCase', 'ChildCase');  -- Only LoneCase and ChildCase

-- Verify the data
SELECT * FROM analysts.idm_and_settlement.SalesforceNoAgentCases




-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##No Assigned Agent Parent Cases

-- COMMAND ----------

-- Create the table
CREATE TABLE IF NOT EXISTS analysts.idm_and_settlement.SalesforceNoParentAgentCases AS
SELECT 
    COALESCE(Owner, 'No Agent Assigned') AS Altered_Agent,  -- Adjusted agent column
    CASE 
        WHEN UPPER(SUBJECT) LIKE '%COMPLEX%' THEN 'Complex Cases'
        ELSE Team
    END AS adjusted_team,  -- Adjusted team column
    *  -- Keep all other columns if needed
FROM 
    analysts.idm_and_settlement.salesforce_all_cases
WHERE 
    STATUS <> 'Closed'  -- Only open cases
    AND COALESCE(Owner, 'No Agent Assigned') = 'No Agent Assigned'  -- Ensuring no agent assigned
    AND CASE 
            WHEN UPPER(SUBJECT) LIKE '%COMPLEX%' THEN 'Complex Cases'
            ELSE Team
        END != 'Complex Cases'  -- Exclude "Complex Cases" team
    AND Case_Relationship IN ('ParentCase'); 

-- Verify the data
SELECT * FROM analysts.idm_and_settlement.SalesforceNoParentAgentCases
--WHERE CASE_NUMBER = '38851607'



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Assigned Cases
-- MAGIC

-- COMMAND ----------

DROP TABLE IF EXISTS analysts.idm_and_settlement.Salesforce_Assigned_Cases;

CREATE TABLE IF NOT EXISTS analysts.idm_and_settlement.Salesforce_Assigned_Cases AS
WITH 
-- 1. Get all no-agent cases (only open cases) with a row number per team, randomizing case order.
No_Agent_Cases AS (
    SELECT 
        SNAC.*,
        CASE 
            WHEN UPPER(SNAC.SUBJECT) LIKE '%COMPLEX%' THEN 'Complex Cases'
            ELSE SNAC.Team
        END AS computed_adjusted_team,
        ROW_NUMBER() OVER (
            PARTITION BY CASE 
                WHEN UPPER(SNAC.SUBJECT) LIKE '%COMPLEX%' THEN 'Complex Cases'
                ELSE SNAC.Team
            END
            ORDER BY RANDOM()  -- Randomizing case order
        ) AS case_rn
    FROM analysts.idm_and_settlement.SalesforceNoAgentCases SNAC
    WHERE COALESCE(SNAC.Owner, 'No Agent Assigned') = 'No Agent Assigned'
      AND CASE 
            WHEN UPPER(SNAC.SUBJECT) LIKE '%COMPLEX%' THEN 'Complex Cases'
            ELSE SNAC.Team
          END != 'Complex Cases'
      AND SNAC.Case_Relationship IN ('LoneCase', 'ChildCase')
      AND SNAC.STATUS <> 'Closed'
),

-- 2. Count the total number of new cases per team.
Team_New_Cases AS (
    SELECT 
        computed_adjusted_team,
        COUNT(*) AS total_new_cases
    FROM No_Agent_Cases
    GROUP BY computed_adjusted_team
),

-- 3. Get agents with workload data, applying a new weight formula.
Agent_Workload AS (
    SELECT 
        Agent,
        adjusted_team,  
        open_case_count,
        Hours_Worked,
        Hours_Worked / (open_case_count + 5) AS weight  
    FROM analysts.idm_and_settlement.Salesforce_Agents
    WHERE Hours_Worked > 0
),

-- 4. Normalize weights per team so they sum to 1.
Agent_Normalized_Weights AS (
    SELECT
        aw.Agent,
        aw.adjusted_team,
        aw.weight / SUM(aw.weight) OVER (PARTITION BY aw.adjusted_team) AS normalized_weight
    FROM Agent_Workload aw
),

-- 5. Compute cumulative weights per team for allocation.
Agent_CumWeights AS (
    SELECT
        anw.Agent,
        anw.adjusted_team,
        anw.normalized_weight,
        SUM(anw.normalized_weight) OVER (PARTITION BY anw.adjusted_team) AS total_weight,
        SUM(anw.normalized_weight) OVER (PARTITION BY anw.adjusted_team ORDER BY anw.Agent) AS cum_weight
    FROM Agent_Normalized_Weights anw
)

-- 6. Assign cases based on cumulative weight ranges.
SELECT 
    nac.*,
    acw.Agent AS Assigned_Agent,
    sa.open_case_count,
    sa.Hours_Worked
FROM No_Agent_Cases nac
JOIN Team_New_Cases tnc 
    ON nac.computed_adjusted_team = tnc.computed_adjusted_team
JOIN Agent_CumWeights acw 
    ON nac.computed_adjusted_team = acw.adjusted_team
LEFT JOIN analysts.idm_and_settlement.Salesforce_Agents sa
    ON acw.Agent = sa.Agent  
    AND acw.adjusted_team = sa.adjusted_team 
WHERE 
    FLOOR(acw.cum_weight * tnc.total_new_cases) >= nac.case_rn 
    AND FLOOR((acw.cum_weight - acw.normalized_weight) * tnc.total_new_cases) < nac.case_rn;

SELECT * FROM analysts.idm_and_settlement.Salesforce_Assigned_Cases

-- COMMAND ----------

-- Drop existing table if it exists
DROP TABLE IF EXISTS analysts.idm_and_settlement.Salesforce_Assigned_Parent_Cases;

-- Create new table
CREATE TABLE analysts.idm_and_settlement.Salesforce_Assigned_Parent_Cases AS
WITH 
-- 1. Get all unassigned ParentCases
Unassigned_ParentCases AS (
    SELECT *
    FROM analysts.idm_and_settlement.salesforce_all_cases
    WHERE 
        STATUS <> 'Closed'
        AND COALESCE(Owner, 'No Agent Assigned') = 'No Agent Assigned'
        AND Case_Relationship = 'ParentCase'
),

-- 2. Get all assigned ChildCases
Assigned_ChildCases AS (
    SELECT *
    FROM analysts.idm_and_settlement.salesforce_all_cases
    WHERE 
        STATUS <> 'Closed'
        AND COALESCE(Owner, 'No Agent Assigned') <> 'No Agent Assigned'
        AND Case_Relationship = 'ChildCase'
),

-- 3. Match unassigned parents to their child cases where teams match
Matched_Children AS (
    SELECT 
        pc.Id AS Parent_ID,
        pc.Case_Number AS Parent_Case_Number,
        pc.Team AS Parent_Team,
        cc.Owner AS Child_Assigned_Agent,
        cc.Team AS Child_Team
    FROM Unassigned_ParentCases pc
    JOIN Assigned_ChildCases cc
        ON cc.ParentID = pc.Id
       AND cc.Team = pc.Team
),

-- 4. Choose one child agent per parent randomly (if multiple, pick randomly)
Random_Agent_Assignments AS (
    SELECT 
        Parent_ID,
        Child_Assigned_Agent,
        ROW_NUMBER() OVER (PARTITION BY Parent_ID ORDER BY RANDOM()) AS rn
    FROM Matched_Children
),

-- 5. Get one agent per parent case
Final_Assignments AS (
    SELECT Parent_ID, Child_Assigned_Agent AS Assigned_Agent
    FROM Random_Agent_Assignments
    WHERE rn = 1
)

-- 6. Join back to parent case and assign agent
SELECT 
    pc.*,
    fa.Assigned_Agent
FROM Unassigned_ParentCases pc
JOIN Final_Assignments fa
    ON pc.Id = fa.Parent_ID;
SELECT * FROM analysts.idm_and_settlement.Salesforce_Assigned_Parent_Cases