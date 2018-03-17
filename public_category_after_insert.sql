--This Postgres Function + Trigger assist in getting around features glue doesn't have such as UPSERT and TRUNCATE TABLE.
--
--Heroku Postgres will use two Schemas:
--  A) public - staging schema where AWS GLUE writes the Redshift rows for the category table.
--         heroku pg:psql -a [app-name]
--         \d salesforce.category__c;

--  redshift-to-salesforce::DATABASE=> \d category;
--          Table "public.category"
--  Column  |         Type          | Modifiers 
-- ----------+-----------------------+-----------
-- catid    | character varying(5)  | not null
-- catgroup | character varying(10) | 
-- catname  | character varying(10) | 
-- catdesc  | character varying(50) | 
__
--  B) salesforce - this is the schema that Heroku Connect uses.
--  redshift-to-salesforce::DATABASE=> \d salesforce.category__c;
--                                           Table "salesforce.category__c"
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

--STEP 1:  A unique index on staging table to prevent duplicates
CREATE unique index public_catid_idx_unq on public.category(catid);

--STEP 2:  After Heroku Connect has synced the Salesforce Object to Postgres, Make sure there is a UNIQUE CONSTRAINT on the column(s)
--         that control the UPSERT.     You can use HEROKU CLI to find this information:

--         You may have to DROP an existing Index on that column if its not unique.   In this example, I am creating a UNIQUE index
--         first using naming convention, then adding the contraint

CREATE unique index catid_unq_idx on salesforce.category__c(catid__c);
ALTER TABLE salesforce.category__c 
   ADD CONSTRAINT catid__c_unq UNIQUE USING INDEX catid_unq_idx;
   

--STEP 3:  This function will upsert the row into the salesforce schema so Heroku Connect can then sync to salesforce; 
--         AWS GLUE nor REDSHIFT support UPSERT 

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


-- STEP 4: After AWS Glue writes a row of data into the staging table public.category - a procedure will be called to update 
--         salesforce.category__c schema which is referenced in Heroku Connect
-- public_category_after_insert
CREATE TRIGGER public_category_after_insert
 AFTER INSERT on public.category
   FOR EACH ROW EXECUTE PROCEDURE public_category_after_insert();

-- Other handy syntax   
-- ALTER TABLE salesforce.category__c drop constraint catid__c_unq;
-- DROP FUNCTION public_category_after_insert();
-- DROP TRIGGER public_category_after_insert ON category;
-- alter table category alter column createddate SET DEFAULT ('now'::text)::TIMESTAMP(6) WITHOUT                                                                            TIME ZONE;

