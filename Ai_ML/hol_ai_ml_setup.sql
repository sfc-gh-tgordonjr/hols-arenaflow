/*
 * Overview of Snowflake Environment Setup Script for ArenaFlow AI/ML Pipelines
 * --------------------------------------------------------------------------------
 * This script establishes a Snowflake environment tailored for ArenaFlow AI/ML pipelines, accommodating both development (DEV) and production (PROD) configurations.
 * Key tasks performed by this script:
 * 1. Validates the Snowflake account region to ensure compatibility with Cortex AI functions, critical for AI/ML operations.
 * 2. Establishes a service role (e.g., DEV_ARENAFLOW_ADMIN or PROD_ARENAFLOW_ADMIN) with necessary privileges for AI/ML pipeline management.
 * 3. Assigns the created role to a specified user for access control.
 * 4. Provisions a compute warehouse and a GPU compute pool, sized appropriately for the environment (e.g., XSMALL for DEV, MEDIUM for PROD).
 * 5. Sets up a database (e.g., DEV_ARENAFLOW or PROD_ARENAFLOW) and schema, including a stage for semantic models.
 * 6. Configures an external access integration with a network rule to enable secure egress to PyPI for package dependencies.
 * 7. Implements governance through a tag (e.g., DEV_ARENAFLOW_TAG or PROD_ARENAFLOW_TAG) and a contact for tracking and support.
 * 8. Applies tags and contact details to relevant objects for consistent governance and traceability.
 *
 * Region Validation Notes:
 * - Cortex AI functions (e.g., SNOWFLAKE.CORTEX.COMPLETE, SENTIMENT, SUMMARIZE) are essential for the ArenaFlow Intelligence Hub notebook.
 * - These functions are available only in select AWS regions, such as aws_us_east_1 (US East, N. Virginia) and aws_us_west_2 (US West, Oregon).
 * - The script uses CURRENT_REGION() to verify the account's region, halting execution with guidance if the region lacks Cortex support.
 * - For more information, consult: https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions
 *
 * Environment Configuration:
 * - Set ENVIRONMENT = 'DEV' for development setup.
 * - Set ENVIRONMENT = 'PROD' for production setup.
 *
 * Customization Instructions:
 * - Modify SF_USER to match your Snowflake username.
 * - Update EMAIL_DISTRIBUTION_LIST with your support email address.
 *
 * Navigation:
 * - Use Command+F to locate sections marked with identifiers like:
 *   - SECTION: REGION CHECK
 *   - SECTION: VARIABLES
 *   - SECTION: SERVICE ROLE
 *   - SECTION: COMPUTE
 *   - SECTION: STORAGE
 *   - SECTION: EXTERNAL ACCESS INTEGRATION
 *   - SECTION: GOVERNANCE TAGS
 *   - SECTION: APPLY TAGS AND CONTACTS
 */


-- SECTION: VARIABLES
------------------------------------------------
-- Define core variables for environment and user configuration
------------------------------------------------
SET ENVIRONMENT = 'DEV';
SET SF_USER = '<SNOWFLAKE USERNAME>';

-- Construct environment-specific names for objects
SET ROLE_NAME = CONCAT($ENVIRONMENT, '_ARENAFLOW_ADMIN');
SET DB_NAME = CONCAT($ENVIRONMENT, '_ARENAFLOW');
SET SCHEMA_NAME = 'AI_ML';
SET WAREHOUSE_NAME = CONCAT($ENVIRONMENT, '_WH_ARENAFLOW_HOLS');
SET COMPUTE_POOL_NAME = CONCAT($ENVIRONMENT, '_GPU_ARENAFLOW_M');
SET STAGE_NAME = 'SEMANTIC_MODELS';
SET NETWORK_RULE_NAME = CONCAT($ENVIRONMENT, '_PYPI_NETWORK_RULE');
SET INTEGRATION_NAME = CONCAT($ENVIRONMENT, '_PYPI_ACCESS_INTEGRATION');
SET TAG_NAME = CONCAT($ENVIRONMENT, '_ARENAFLOW_TAG');

-- SECTION: SERVICE ROLE
------------------------------------------------
-- Establish and configure a role for administering AI/ML pipelines
------------------------------------------------
USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE ROLE IDENTIFIER($ROLE_NAME);
GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE IDENTIFIER($ROLE_NAME);

USE ROLE SYSADMIN;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE IDENTIFIER($ROLE_NAME);
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE IDENTIFIER($ROLE_NAME);
GRANT CREATE COMPUTE POOL ON ACCOUNT TO ROLE IDENTIFIER($ROLE_NAME);
GRANT EXECUTE TASK ON ACCOUNT TO ROLE IDENTIFIER($ROLE_NAME);
GRANT ROLE IDENTIFIER($ROLE_NAME) TO USER IDENTIFIER($SF_USER);
------------------------------------------------
-- SECTION: COMPUTE
------------------------------------------------
-- Provision compute resources tailored to the environment
------------------------------------------------
USE ROLE IDENTIFIER($ROLE_NAME);
CREATE WAREHOUSE IF NOT EXISTS IDENTIFIER($WAREHOUSE_NAME)
    WAREHOUSE_SIZE ='X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;
CREATE COMPUTE POOL IDENTIFIER($COMPUTE_POOL_NAME)
    MIN_NODES = 1
    MAX_NODES = 2
    INSTANCE_FAMILY = GPU_NV_M;
-- Assign ownership and usage rights for compute resources
GRANT OWNERSHIP ON WAREHOUSE IDENTIFIER($WAREHOUSE_NAME) TO ROLE IDENTIFIER($ROLE_NAME);
GRANT OWNERSHIP ON COMPUTE POOL IDENTIFIER($COMPUTE_POOL_NAME) TO ROLE IDENTIFIER($ROLE_NAME);

------------------------------------------------
-- SECTION: STORAGE
------------------------------------------------
-- Set up storage structures for AI/ML data and models
------------------------------------------------
USE ROLE IDENTIFIER($ROLE_NAME);
CREATE DATABASE IF NOT EXISTS IDENTIFIER($DB_NAME);
CREATE SCHEMA IF NOT EXISTS IDENTIFIER($SCHEMA_NAME);
CREATE STAGE IF NOT EXISTS IDENTIFIER($STAGE_NAME);

-- SECTION: REGION CHECK
------------------------------------------------
-- Verify region compatibility for Cortex AI functions
-- 1) Define variables and exception for region validation
-- 2) Compare current region against supported list
-- 3) Halt with guidance if region is unsupported
CREATE OR REPLACE PROCEDURE CHECK_SUPPORTED_REGION()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'handler_function'
AS
$$
def handler_function(session):
    # List of regions supporting Cortex AI functions
    SUPPORTED_REGIONS = ['AWS_US_EAST_1', 'AWS_US_WEST_2']

    # Retrieve the current region from Snowflake
    result = session.sql("SELECT CURRENT_REGION()").collect()
    full_region = result[0][0]  # Access the first column in the first row

    # Parse the region name for comparison
    current_region = full_region.split('.')[-1].upper()

    # Validate region against supported list
    if current_region not in SUPPORTED_REGIONS:
        raise Exception(f"Cortex AI functions are not supported in your current region: {current_region}. "
                        "Please use an account in one of the supported regions: AWS_US_EAST_1 (US East, N. Virginia) or AWS_US_WEST_2 (US West, Oregon). "
                        "Refer to https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions for details.")

    return "Your account is in a supported region: " + current_region
$$;

-- Execute the region validation procedure
CALL CHECK_SUPPORTED_REGION();

------------------------------------------------
-- SECTION: EXTERNAL ACCESS INTEGRATION
------------------------------------------------
-- Enable secure egress to PyPI for package dependencies
------------------------------------------------
USE ROLE ACCOUNTADMIN;
-- Grant permission to create network rules and integrations
GRANT CREATE NETWORK RULE ON SCHEMA IDENTIFIER($SCHEMA_NAME) TO ROLE IDENTIFIER($ROLE_NAME);

-- Define a network rule for PyPI egress
USE ROLE IDENTIFIER($ROLE_NAME);
CREATE OR REPLACE NETWORK RULE IDENTIFIER($NETWORK_RULE_NAME)
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = (
        'pypi.org',
        'pypi.python.org',
        'pythonhosted.org',
        'files.pythonhosted.org'
    );
-- Create an integration for controlled external access
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION IDENTIFIER($INTEGRATION_NAME)
ALLOWED_NETWORK_RULES = ($NETWORK_RULE_NAME) 
ENABLED = TRUE;

-- Allow the admin role to use the integration
GRANT USAGE ON INTEGRATION IDENTIFIER($INTEGRATION_NAME) TO ROLE IDENTIFIER($ROLE_NAME);

------------------------------------------------
-- SECTION: GOVERNANCE TAGS
------------------------------------------------
-- Define governance tags for tracking and management
------------------------------------------------
USE ROLE IDENTIFIER($ROLE_NAME);
CREATE OR REPLACE TAG IDENTIFIER($TAG_NAME)
    ALLOWED_VALUES 'ARENAFLOW_AI'
    COMMENT = 'Tag to categorize workloads for tracking AI/ML pipelines';

------------------------------------------------
-- SECTION: APPLY TAGS AND CONTACTS
------------------------------------------------
-- Apply governance tags to relevant objects
------------------------------------------------
ALTER WAREHOUSE IDENTIFIER($WAREHOUSE_NAME) SET TAG IDENTIFIER($TAG_NAME) = 'ARENAFLOW_AI';
ALTER COMPUTE POOL IDENTIFIER($COMPUTE_POOL_NAME) SET TAG IDENTIFIER($TAG_NAME) = 'ARENAFLOW_AI';
ALTER DATABASE IDENTIFIER($DB_NAME) SET TAG IDENTIFIER($TAG_NAME) = 'ARENAFLOW_AI';
ALTER SCHEMA IDENTIFIER($SCHEMA_NAME) SET TAG IDENTIFIER($TAG_NAME) = 'ARENAFLOW_AI';