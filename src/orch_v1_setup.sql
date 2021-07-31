-- Set up SQL resources in Snowflake for SnowflakeAsyncOrchestrator demo.
CREATE DATABASE IF NOT EXISTS "ALGO_DEMO";

USE DATABASE "ALGO_DEMO";

CREATE SCHEMA IF NOT EXISTS "ORCH_V1";

USE SCHEMA "ORCH_V1";

-- Drop test tables to start with clean slate.
DROP TABLE IF EXISTS "ALGO_DEMO"."ORCH_V1"."TEST_INPUT_TABLE";
DROP TABLE IF EXISTS "ALGO_DEMO"."ORCH_V1"."TEST_OUTPUT_TABLE";

-- Create test tables.
CREATE OR REPLACE TABLE "TEST_INPUT_TABLE" (
  batch_id integer not null,
  lead_id varchar(100),
  lead_zip varchar(20),
  is_licensed boolean default false
);

CREATE OR REPLACE TABLE "TEST_OUTPUT_TABLE" (
  lead_id varchar(100),
  score_value integer,
  score_tstamp varchar(20)
);

-- Seed dummy data.
INSERT INTO "ALGO_DEMO"."ORCH_V1"."TEST_INPUT_TABLE" (
  batch_id, lead_id, lead_zip, is_licensed)
VALUES (1, '0008NIAUDRT', 98101, TRUE),
       (2, '0008DUBKD48', 98102, TRUE),
       (2, '0008DOIQWFF', 98101, FALSE),
       (2, '0008DPCVFET', 98101, FALSE);

-- View data.
SELECT * FROM "ALGO_DEMO"."ORCH_V1"."TEST_INPUT_TABLE";
SELECT * FROM "ALGO_DEMO"."ORCH_V1"."TEST_OUTPUT_TABLE";