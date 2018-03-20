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
