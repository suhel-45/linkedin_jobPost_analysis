CREATE OR REPLACE TABLE jobpostdb.linkedin_job_schema.company_dim(
    company_key NUMBER AUTOINCREMENT PRIMARY KEY,
    id BIGINT,
    company_id NUMBER,
    company_name VARCHAR,
    company_url VARCHAR
);
ALTER TABLE  jobpostdb.linkedin_job_schema.company_dim
ADD CONSTRAINT unique_cmp_id UNIQUE(company_id);




CREATE OR REPLACE TABLE jobpostdb.linkedin_job_schema.job_dim(
    job_key NUMBER AUTOINCREMENT PRIMARY KEY,
    id BIGINT,
    title VARCHAR,
    job_url VARCHAR,
    type VARCHAR,
    location VARCHAR
);
ALTER TABLE jobpostdb.linkedin_job_schema.job_dim
ADD CONSTRAINT unique_job UNIQUE(id,title,job_url);




CREATE OR REPLACE TABLE  jobpostdb.linkedin_job_schema.date_dim(
    date_key NUMBER AUTOINCREMENT PRIMARY KEY,
    id BIGINT,
    date date,
    year INTEGER,
    month VARCHAR,
    dayOfMonth INTEGER,
    dayOfWeek VARCHAR
);
ALTER TABLE jobpostdb.linkedin_job_schema.date_dim
ADD CONSTRAINT unique_date UNIQUE(date);




CREATE OR REPLACE TABLE  jobpostdb.linkedin_job_schema.post_fact(
    post_id NUMBER AUTOINCREMENT PRIMARY KEY,
    id BIGINT,
    referenceId VARCHAR,
    company_key NUMBER,
    job_key NUMBER,
    date_key NUMBER,
    FOREIGN KEY (company_key) REFERENCES jobpostdb.linkedin_job_schema.company_dim(company_key),
    FOREIGN KEY (job_key) REFERENCES jobpostdb.linkedin_job_schema.job_dim(job_key),
    FOREIGN KEY (date_key) REFERENCES jobpostdb.linkedin_job_schema.date_dim(date_key)
);
ALTER TABLE jobpostdb.linkedin_job_schema.post_fact
ADD CONSTRAINT unique_post UNIQUE(id,referenceId);
