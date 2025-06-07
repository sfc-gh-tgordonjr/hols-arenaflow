/*
 * Setup Script for Dynamic Tables Lab - ArenaFlow Stadium Operations Hub
 * ------------------------------------------------------------------------
 * This script configures the Snowflake environment for the Dynamic Tables hands-on lab.
 * It creates the service role, database, schema, warehouse, and governance tags.
 * 
 * Key tasks performed by this script:
 * 1. Establishes a service role with necessary privileges for dynamic table management.
 * 2. Assigns the created role to a specified user for access control.
 * 3. Provisions a compute warehouse for the Dynamic Tables lab.
 * 4. Sets up a database and schema for supporting dynamic tables.
 * 5. Implements governance through a tag and applies it to relevant objects.
 *
 * Environment Configuration:
 * - Set ENVIRONMENT = 'DEV' for development setup.
 * - Update SF_USER to match your Snowflake username.
 * 
 * Navigation:
 * - Use Command+F to locate sections marked with identifiers like:
 *   - SECTION: VARIABLES
 *   - SECTION: SERVICE ROLE
 *   - SECTION: COMPUTE
 *   - SECTION: STORAGE
 *   - SECTION: GOVERNANCE TAGS
 */

/* SECTION: VARIABLES ----------------------------------------------------- */
-- Define core variables for environment configuration
SET ENVIRONMENT = 'DEV';
SET SF_USER = '<SNOWFLAKE USERNAME>';

-- Construct names based on environment
SET ROLE_NAME = CONCAT($ENVIRONMENT, '_ARENAFLOW_ADMIN');
SET DB_NAME = CONCAT($ENVIRONMENT, '_ARENAFLOW');
SET SCHEMA_NAME = 'DYNAMIC_TABLES';
SET WAREHOUSE_NAME = CONCAT($ENVIRONMENT, '_WH_ARENAFLOW_HOLS');
SET TAG_NAME = CONCAT($ENVIRONMENT, '_ARENAFLOW_TAG');

/* SECTION: SERVICE ROLE -------------------------------------------------- */
-- Create and grant privileges for the admin role
USE ROLE ACCOUNTADMIN;
CREATE ROLE IF NOT EXISTS IDENTIFIER($ROLE_NAME);
GRANT CREATE DATABASE ON ACCOUNT TO ROLE IDENTIFIER($ROLE_NAME);
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE IDENTIFIER($ROLE_NAME);
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE IDENTIFIER($DB_NAME) TO ROLE IDENTIFIER($ROLE_NAME);
GRANT ROLE IDENTIFIER($ROLE_NAME) TO USER IDENTIFIER($SF_USER);

/* SECTION: COMPUTE -------------------------------------------------------- */
-- Provision a warehouse for the Dynamic Tables lab
USE ROLE IDENTIFIER($ROLE_NAME);
CREATE OR REPLACE WAREHOUSE IDENTIFIER($WAREHOUSE_NAME)
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

/* SECTION: STORAGE ------------------------------------------------------- */
-- Create database and schema for dynamic tables
USE ROLE IDENTIFIER($ROLE_NAME);
CREATE DATABASE IF NOT EXISTS IDENTIFIER($DB_NAME);
CREATE SCHEMA IF NOT EXISTS IDENTIFIER($SCHEMA_NAME);

/* SECTION: GOVERNANCE TAGS ----------------------------------------------- */
-- Define and apply governance tags
USE ROLE IDENTIFIER($ROLE_NAME);
CREATE TAG IF NOT EXISTS IDENTIFIER($TAG_NAME)
    ALLOWED_VALUES 'DYNAMIC_TABLES_DEMO'
    COMMENT = 'Tag for Dynamic Tables Hands-On Lab';

ALTER DATABASE IDENTIFIER($DB_NAME)
    SET TAG IDENTIFIER($TAG_NAME) = 'DYNAMIC_TABLES_DEMO';

ALTER SCHEMA IDENTIFIER($SCHEMA_NAME)
    SET TAG IDENTIFIER($TAG_NAME) = 'DYNAMIC_TABLES_DEMO';

ALTER WAREHOUSE IDENTIFIER($WAREHOUSE_NAME)
    SET TAG IDENTIFIER($TAG_NAME) = 'DYNAMIC_TABLES_DEMO';

/* END OF SETUP SCRIPT ---------------------------------------------------- */