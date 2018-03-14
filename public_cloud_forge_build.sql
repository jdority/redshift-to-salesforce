--This Postgres Function + Trigger assist in getting around features glue doesn't have such as UPSERT and TRUNCATE TABLE.
--
--Heroku Postgres will use two Schemas:
--  A) public - staging schema where AWS GLUE writes the Redshift or RDS rows for the cloud_forge_build table.
--         heroku pg:psql -a [app-name]

--  redshift-to-salesforce::DATABASE=> \d cloud_forge_build;
--  Table "public.cloud_forge_build"
--      Column       |         Type          | Modifiers 
-- -------------------+-----------------------+-----------
-- name              | character varying(80) | not null
-- site              | character varying(10) | 
-- service_positions | integer               | 


--  B) salesforce - this is the schema that Heroku Connect uses.
--  redshift-to-salesforce::DATABASE=> \d salesforce.cloud_forge_build_test__c;
-- Table "salesforce.cloud_forge_build_test__c"
--        Column        |            Type             |                                     Modifiers                                     
-- ----------------------+-----------------------------+-----------------------------------------------------------------------------------
-- createddate          | timestamp without time zone | 
-- isdeleted            | boolean                     | 
-- name                 | character varying(80)       | 
-- systemmodstamp       | timestamp without time zone | 
-- service_positions__c | double precision            | 
-- site__c              | character varying(10)       | 
-- sfid                 | character varying(18)       | 
-- id                   | integer                     | not null default nextval('salesforce.cloud_forge_build_test__c_id_seq'::regclass)
-- _hc_lastop           | character varying(32)       | 
-- _hc_err              | text                        | 
-- shadow_name__c       | character varying(80)       | 
-- Indexes:
--   "cloud_forge_build_test__c_pkey" PRIMARY KEY, btree (id)      \* Heroku Connect adds a Postgres Serial Number as it inserts rows from Salesforce to Postgres */
--   "hcu_idx_cloud_forge_build_test__c_sfid" UNIQUE, btree (sfid) \* Heroku Connect adds a unique index on the sfid which is the Salesforce ID column for the object. */
-- -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--STEP 1:  A unique index on staging table to prevent duplicates
CREATE unique index public_name_idx_unq on public.cloud_forge_build(name);

--STEP 2:  After Heroku Connect has synced the Salesforce Object to Postgres, Make sure there is a UNIQUE CONSTRAINT on the column(s)
--         In this example, I am creating a UNIQUE index first using my favorite naming convention, then adding the contraint using this index.

CREATE unique index name_unq_idx on salesforce.cloud_forge_build_test__c(name);
ALTER TABLE salesforce.cloud_forge_build_test__c 
   ADD CONSTRAINT name_unq UNIQUE USING INDEX name_unq_idx;
   

--STEP 3:  This function will upsert the row into the salesforce schema so Heroku Connect can then sync to salesforce; 
--         AWS GLUE nor REDSHIFT support UPSERT 

CREATE OR REPLACE FUNCTION public_cloud_forge_build_test_after_insert()
    RETURNS trigger AS

    $BODY$
        BEGIN
            IF pg_trigger_depth() <> 1 THEN
                RETURN NEW;
            END IF;
            INSERT INTO salesforce.cloud_forge_build_test__c
                   (id, 
                   name, 
                   site__c, 
                   service_positions__c, 
                   shadow_name__c)    /* shadow is used because Salesforce won't allow the name column to be an External ID */
               VALUES 
                   (NEXTVAL('salesforce.cloud_forge_build_test__c_id_seq'::regclass),
                    NEW.name, 
                    NEW.site, 
                    NEW.service_positions, 
                    NEW.name)                                      /* this maps to shadow_name__c */
                 ON CONFLICT ON CONSTRAINT name_unq -- UPSERT
                    DO UPDATE SET 
                       (site__c, 
                       service_positions__c) = 

                       (NEW.site, 
                       NEW.service_positions);
                 ------------------------------------------------------------------------------------------------
                 -- This statement will remove row from staging table public.cloud_forge_build AFTER the UPSERT completes,
                 -- again because AWS Glue cannot TRUNCATE a table, it can only re-create

                 DELETE from cloud_forge_build where name = NEW.name;
                 ------------------------------------------------------------------------------------------------
               RETURN NULL;
         END; 
    $BODY$
    LANGUAGE plpgsql;

-- STEP 4: Add trigger to staging table so that the FUNCTION is called to push rows to salesforce schema
 CREATE TRIGGER public_cloud_forge_build_test_after_insert
 AFTER INSERT on public.cloud_forge_build
   FOR EACH ROW EXECUTE PROCEDURE public_cloud_forge_build_test_after_insert();
