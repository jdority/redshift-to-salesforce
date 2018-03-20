--This Postgres Function + Trigger assist in getting around features glue doesn't have such as UPSERT and TRUNCATE TABLE.
--
--Heroku Postgres will use two Schemas which need to be IDENTICAL:
--  A) public - staging schema where AWS GLUE writes the Redshift rows for the category table.
--         heroku pg:psql -a [app-name]
--         \d salesforce.category__c;
__
--  B) salesforce - this is the schema that Heroku Connect uses.
--     \d salesforce.category__c;
--   
--    Column     |            Type             |                              Modifiers                              
-- ----------------+-----------------------------+---------------------------------------------------------------------
-- createddate    | timestamp without time zone | 
-- isdeleted      | boolean                     | 
-- name           | character varying(80)       | 
-- systemmodstamp | timestamp without time zone | 
-- catdesc__c     | character varying(50)       | 
-- sfid           | character varying(18)       | 
-- id             | integer                     | not null default nextval('salesforce.category__c_id_seq'::regclass)
-- _hc_lastop     | character varying(32)       | 
-- _hc_err        | text                        | 
-- catgroup__c    | character varying(10)       | 
-- catid__c       | character varying(5)        | 
-- Indexes:
--   "category__c_pkey" PRIMARY KEY, btree (id)                 /* Heroku Connect creates a field called ID which is a Postgres serial number for each row it syncs to Postgres */
--   "catid__c_unq" UNIQUE CONSTRAINT, btree (catid__c)         /* This field is the real application UNIQUE Key so a UNIQUE CONSTRAINT is placed on this column.
--   "hcu_idx_category__c_sfid" UNIQUE, btree (sfid)            /* Heroku Connect also places a UNIQUE INDEX on the sfid (or ID in Salesforce)
--   "hc_idx_category__c_systemmodstamp" btree (systemmodstamp)
-- -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--STEP 1:  A unique index on staging table to prevent duplicates when GLUE inserts
CREATE unique index public_catid_idx_unq on public.category(catid);

--STEP 2:  After Heroku Connect has synced the Salesforce Object to Postgres, Make sure there is a UNIQUE CONSTRAINT on the column(s)
--         that control the UPSERT.     You can use HEROKU CLI to find this information:

--         You may have to DROP an existing Index on that column if its not unique.   In this example, I am creating a UNIQUE index
--         first using naming convention, then adding the contraint

CREATE unique index catid_unq_idx on salesforce.category__c(catid__c);
ALTER TABLE salesforce.category__c 
   ADD CONSTRAINT catid__c_unq UNIQUE USING INDEX catid_unq_idx;
   

--STEP 3:  This function will write to the salesforce schema so Heroku Connect can then sync to salesforce; 
--         AWS GLUE nor REDSHIFT support UPSERT 
CREATE OR REPLACE FUNCTION public_category_after_insert()
    RETURNS trigger AS
    
    $BODY$
      DECLARE cnt integer;

        BEGIN
            IF pg_trigger_depth() <> 1 THEN
                RETURN NEW;
            END IF;
          
            SELECT COUNT(*) INTO cnt FROM salesforce.category__c WHERE catid__c = NEW.catid__c;
            IF cnt = 1 THEN   
                 -- UPSERT Syntax is inflexible - can't update all columns, must list by name.   
                 -- To reduce maintenance a delete/insert is used.    These should execute quickly since indexed.
                 DELETE FROM salesforce.category__c where catid__c = NEW.catid__c;
            END IF;
            
            -- Postgres does not automatically provide the next number in a sequence if the field is NULL, so must call NEXTVAL
            NEW.id = NEXTVAL('salesforce.category__c_id_seq'::regclass);
            INSERT INTO salesforce.category__c values (NEW.*);
           
            RETURN NULL; 

        -- ----------------------------------------------------------------------------------------------
        -- This statement will remove row from staging table public.category AFTER the UPSERT completes,
        -- again because AWS Glue cannot TRUNCATE a table, it can only re-create
        -- DELETE FROM category WHERE catid__c = NEW.catid__c;
                 ------------------------------------------------------------------------------------------------
         END; 
    $BODY$
    LANGUAGE plpgsql;


-- STEP 4: After AWS Glue writes a row of data into the staging table public.category - a procedure will be called to update 
--         salesforce.category__c schema which is referenced in Heroku Connect

CREATE TRIGGER public_category_after_insert
 AFTER INSERT on public.category
   FOR EACH ROW EXECUTE PROCEDURE public_category_after_insert();

-- Other handy syntax   
-- ALTER TABLE salesforce.category__c drop constraint catid__c_unq;
-- DROP FUNCTION public_category_after_insert();
-- DROP TRIGGER public_category_after_insert ON category;
-- ALTER TABLE category ALTER COLUMN createddate SET DEFAULT ('now'::text)::TIMESTAMP(6) WITHOUT TIME ZONE; 
-- CURRVAL('salesforce.category__c_id_seq'::regclass) + 1)  
-- NEXTVAL('salesforce.category__c_id_seq'::regclass) + 1)  


