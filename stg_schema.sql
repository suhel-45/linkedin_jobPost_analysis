CREATE OR REPLACE TABLE jobpostdb.linkedin_job_stg_schema.stg_dim_company(
    id BIGINT,
    company_id NUMBER,
    company_name VARCHAR,
    company_url VARCHAR
);


CREATE OR REPLACE TABLE  jobpostdb.linkedin_job_stg_schema.stg_dim_job(
    id BIGINT,
    title VARCHAR,
    job_url VARCHAR,
    type VARCHAR,
    location VARCHAR
);



CREATE OR REPLACE TABLE jobpostdb.linkedin_job_stg_schema.stg_dim_date(
    id BIGINT,
    date date,
    year INTEGER,
    month VARCHAR,
    dayOfMonth INTEGER,
    dayOfWeek VARCHAR
);


CREATE OR REPLACE TABLE jobpostdb.linkedin_job_stg_schema.stg_fact_post(
    id BIGINT,
    referenceId VARCHAR
);
