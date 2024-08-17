
--- CREATING STREAM FOR STAGE TABLE stg_dim_company
CREATE OR REPLACE STREAM jobpostdb.LINKEDIN_STG_STREAM_SCHEMA.stg_comp_stream 
ON TABLE jobpostdb.linkedin_job_stg_schema.stg_dim_company;

--- CREATING STREAM FOR STAGE TABLE stg_dim_job
CREATE OR REPLACE STREAM jobpostdb.LINKEDIN_STG_STREAM_SCHEMA.stg_job_stream
ON TABLE jobpostdb.linkedin_job_stg_schema.stg_dim_job;

--- CREATING STREAM FOR STAGE TABLE stg_dim_date
CREATE OR REPLACE STREAM  jobpostdb.LINKEDIN_STG_STREAM_SCHEMA.stg_date_stream
ON TABLE jobpostdb.linkedin_job_stg_schema.stg_dim_date;

--- CREATING STREAM FOR STAGE TABLE stg_fact_post
CREATE OR REPLACE STREAM  jobpostdb.LINKEDIN_STG_STREAM_SCHEMA.stg_post_stream
ON TABLE jobpostdb.linkedin_job_stg_schema.stg_fact_post;


---CREATING TASK TO MERGE THE DATA FROM STAGE TABLES TO WAREHOUSE TABLE

CREATE OR REPLACE TASK jobpostdb.task_schema.task
WAREHOUSE = DATAVISTA
WHEN
    SYSTEM$STREAM_HAS_DATA('jobpostdb.LINKEDIN_STG_STREAM_SCHEMA.stg_comp_stream')
    OR
    SYSTEM$STREAM_HAS_DATA('jobpostdb.LINKEDIN_STG_STREAM_SCHEMA.stg_job_stream')
    OR
    SYSTEM$STREAM_HAS_DATA('jobpostdb.LINKEDIN_STG_STREAM_SCHEMA.stg_date_stream')
    OR
    SYSTEM$STREAM_HAS_DATA('jobpostdb.LINKEDIN_STG_STREAM_SCHEMA.stg_post_stream')
AS
BEGIN
    -- MERGING TABLE 1
    MERGE INTO jobpostdb.linkedin_job_schema.company_dim tgt
    USING jobpostdb.linkedin_job_stg_schema.stg_dim_company src
    ON tgt.id = src.id
    WHEN NOT MATCHED THEN
        INSERT(id, company_id, company_name, company_url)
        VALUES(src.id, src.company_id, src.company_name, src.company_url);
    

    -- MERGING TABLE 2
    MERGE INTO jobpostdb.linkedin_job_schema.job_dim tgt
    USING jobpostdb.linkedin_job_stg_schema.stg_dim_job AS src
    ON tgt.id = src.id
    WHEN NOT MATCHED THEN
        INSERT(id, title, job_url, type,location)
        VALUES(src.id, src.title, src.job_url, src.type,src.location);
    
    -- MERGING TABLE 3
    MERGE INTO jobpostdb.linkedin_job_schema.date_dim tgt
    USING jobpostdb.linkedin_job_stg_schema.stg_dim_date src
    ON tgt.id = src.id
    WHEN NOT MATCHED THEN
        INSERT(id, date, year, month, dayOfMonth, dayOfWeek)
        VALUES(src.id, src.date, src.year, src.month, src.dayOfMonth, src.dayOfWeek);

    -- MERGING TABLE 4
    MERGE INTO jobpostdb.linkedin_job_schema.post_fact tgt
    USING (
        SELECT src1.id AS id,
               src1.company_key AS company_key,
               src2.job_key AS job_key,
               src3.date_key AS date_key,
               stg.referenceId AS referenceId
        FROM jobpostdb.linkedin_job_schema.company_dim src1
        JOIN jobpostdb.linkedin_job_schema.job_dim src2 ON src1.id = src2.id
        JOIN jobpostdb.linkedin_job_schema.date_dim src3 ON src2.id = src3.id
        JOIN jobpostdb.linkedin_job_stg_schema.stg_fact_post stg ON src3.id = stg.id
    ) src
    ON tgt.id = src.id
    WHEN MATCHED AND(
        tgt.company_key != src.company_key OR
        tgt.job_key != src.job_key OR
        tgt.date_key != src.date_key)  
        THEN
        UPDATE SET
            tgt.company_key = src.company_key,
            tgt.job_key = src.job_key,
            tgt.date_key = src.date_key
            
    WHEN NOT MATCHED THEN
        INSERT(id, referenceId, company_key, job_key, date_key)
        VALUES(src.id, src.referenceId, src.company_key, src.job_key, src.date_key);                  
    DELETE FROM  jobpostdb.linkedin_job_stg_schema.stg_dim_company;
    DELETE FROM  jobpostdb.linkedin_job_stg_schema.stg_dim_job;
    DELETE FROM  jobpostdb.linkedin_job_stg_schema.stg_dim_date;
    DELETE FROM  jobpostdb.linkedin_job_stg_schema.stg_fact_post;    
   
    
    COMMIT;
   
END;

ALTER TASK JOBPOSTDB.TASK_SCHEMA.task RESUME;
