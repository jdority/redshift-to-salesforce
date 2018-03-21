--This Postgres Function + Trigger assist in getting around features glue doesn't have such as UPSERT and TRUNCATE TABLE.
--
--Heroku Postgres will use two Schemas which need to be IDENTICAL (except for id NEXVAL.   Only salesforce schema uses this):
--  A) public - staging schema where AWS GLUE writes the Redshift rows for the category table.
--         heroku pg:psql -a sfdc-etl
--         \d cloud_forge_build_test;
--         \d salesforce.cloud_forge_build_test__c;   /*NOTE THE 'test' convention*/
__
--  B) salesforce - this is the schema that Heroku Connect uses.

create table cloud_forge_build_test
(createddate           timestamp without time zone,
 isdeleted             boolean,  
 name                  character varying(80),    
 systemmodstamp        timestamp without time zone,
 service_positions__c  double precision, 
 site__c               character varying(10),        
 sfid                  character varying(18),       
 id                    integer,             --- identical to heroku/salesforce schema EXCEPT YOU DON'T NEED NEXTVAL        
 _hc_lastop            character varying(32),        
 _hc_err               text,                       
 shadow_name__c        character varying(80)
 );      


--STEP 1:  A unique index on staging table to prevent duplicates
CREATE unique index public_name_idx_unq on public.cloud_forge_build_test(name);
   
--STEP 2:  This function will write to the salesforce schema so Heroku Connect can then sync to salesforce; 
--         AWS GLUE nor REDSHIFT support UPSERT 
CREATE OR REPLACE FUNCTION public_cloud_forge_build_test_insert()
    RETURNS trigger AS
    
    $BODY$
      DECLARE cnt integer;

        BEGIN
            IF pg_trigger_depth() <> 1 THEN
                RETURN NEW;
            END IF;
          
            SELECT COUNT(*) INTO cnt FROM salesforce.cloud_forge_build_test__c WHERE name = NEW.name;
            IF cnt = 1 THEN   
                 -- UPSERT Syntax is inflexible - can't update all columns, must list by name.   
                 -- To reduce maintenance a delete/insert is used.    These should execute quickly since indexed.
                 DELETE FROM salesforce.cloud_forge_build_test__c where name = NEW.name;
            END IF;
            
            -- Postgres does not automatically provide the next number in a sequence if the field is NULL, so must call NEXTVAL
            NEW.id = NEXTVAL('salesforce.cloud_forge_build_test__c_id_seq'::regclass);
            NEW.shadow_name__c = NEW.name;
            INSERT INTO salesforce.cloud_forge_build_test__c values (NEW.*);
           
            RETURN NULL; 
        -- ----------------------------------------------------------------------------------------------
        -- This statement will remove row from staging table public.category AFTER the UPSERT completes,
        -- again because AWS Glue cannot TRUNCATE a table, it can only re-create
           DELETE FROM category WHERE catid__c = NEW.catid__c;
        -- ----------------------------------------------------------------------------------------------
         END; 
    $BODY$
    LANGUAGE plpgsql;

-- STEP 3: After AWS Glue writes a row of data into the staging table the procedure will be called to update the 
--         Heroku Connect Connect managed table

CREATE TRIGGER public_cloud_forge_build_test_after_insert
 AFTER INSERT on public.cloud_forge_build_test
   FOR EACH ROW EXECUTE PROCEDURE public_cloud_forge_build_test_insert();
   
   -- Other handy Postgres syntax   
-- ALTER TABLE salesforce.category__c drop constraint catid__c_unq;
-- DROP FUNCTION public_category_after_insert();
-- DROP TRIGGER public_category_after_insert ON category;
-- ALTER TABLE category ALTER COLUMN createddate SET DEFAULT ('now'::text)::TIMESTAMP(6) WITHOUT TIME ZONE; 
-- CURRVAL('salesforce.category__c_id_seq'::regclass) + 1)  
-- NEXTVAL('salesforce.category__c_id_seq'::regclass)  

