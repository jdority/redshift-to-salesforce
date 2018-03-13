--This Postgres Function + Trigger assist in getting around features glue doesn't have such as UPSERT and TRUNCATE TABLE.
--
--Heroku Postgres Schemas:
--  * public - staging schema where AWS GLUE writes the Redshift rows for the category table.
--  * salesforce - this is the schema that Heroku Connect uses.


--STEP 1:  A unique constraint is required to use ON CONFLICT syntax.  Heroku Connect creates with hcu_ naming convention.
DROP index salesforce.hc_idx_cloud_forge_build_test__c_name; - Drop Index and recreate as unique
CREATE unique index salesforce_name_idx_unq on salesforce.cloud_forge_build_test__c (name);
CREATE unique index public_name_idx_unq on public.cloud_forge_build(name);
ALTER TABLE salesforce.category__c 
   ADD CONSTRAINT catid__c_unq UNIQUE USING INDEX hcu_idx_category__c_catid__c;
   

--STEP 2:  This function will upsert the row into the salesforce schema so Heroku Connect can then sync to salesforce; 
--         AWS does not support UPSERT 
--public_category_after_insert()
CREATE OR REPLACE FUNCTION public_category_after_insert()
    RETURNS trigger AS

    $BODY$
        BEGIN
            IF pg_trigger_depth() <> 1 THEN
                RETURN NEW;
            END IF;
            INSERT INTO salesforce.category__c(id, catid__c, catgroup__c, name, catdesc__c)
                 VALUES (NEXTVAL('salesforce.category__c_id_seq'::regclass),NEW.catid, NEW.catgroup, NEW.catname, NEW.catdesc)
                 ON CONFLICT ON CONSTRAINT catid__c_unq -- UPSERT
                 DO UPDATE SET (catgroup__c, name, catdesc__c) = (NEW.catgroup, NEW.catname, NEW.catdesc);
                 ------------------------------------------------------------------------------------------------
                 -- This statement will remove row from staging table public.category AFTER the UPSERT completes,
                 -- again because AWS Glue cannot TRUNCATE a table, it can only re-create
                 DELETE from category where catid = NEW.catid;
                 ------------------------------------------------------------------------------------------------
               RETURN NULL;
         END; 
    $BODY$
    LANGUAGE plpgsql;


-- STEP 3: After AWS Glue writes a row of data into the staging table public.category - a procedure will be called to update 
--         salesforce.category__c schema which is referenced in Heroku Connect
-- public_category_after_insert
CREATE TRIGGER public_category_after_insert
 AFTER INSERT on public.category
   FOR EACH ROW EXECUTE PROCEDURE public_category_after_insert();

-- Other handy syntax   
-- DROP FUNCTION public_category_after_insert();
-- DROP TRIGGER public_category_after_insert ON category;
