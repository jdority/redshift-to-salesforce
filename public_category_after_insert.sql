#
# Heroku Postgres Schemas:
#    * public - staging schema where AWS GLUE writes the Redshift rows for the category table.
#    * salesforce - this is the schema that Heroku Connect uses.
# 
# After AWS Glue writes a row to the public staging schema, this function will upsert the row into the salesforce schema
# so Heroku Connect can then sync to salesforce;
#
# A unique constraint is required to use ON CONFLICT syntax.   Heroku Connect creates them with hcu_ naming convention.
# ALTER TABLE salesforce.category__c 
#   ADD CONSTRAINT catid__c_unq UNIQUE USING INDEX hcu_idx_category__c_catid__c;
#
# public_category_after_insert()

CREATE OR REPLACE FUNCTION public_category_after_insert()
    RETURNS trigger AS

    $BODY$
        BEGIN
            IF pg_trigger_depth() <> 1 THEN
                RETURN NEW;
            END IF;
            INSERT INTO salesforce.category__c(id, catid__c, catgroup__c, name, catdesc__c)
                 VALUES (NEXTVAL('salesforce.category__c_id_seq'::regclass),NEW.catid, NEW.catgroup, NEW.catname, NEW.catdesc)
                 ON CONFLICT ON CONSTRAINT catid__c_unq
                 DO UPDATE SET (catgroup__c, name, catdesc__c) = (NEW.catgroup, NEW.catname, NEW.catdesc);
                 DELETE from category where catid = NEW.catid;
               RETURN NULL;
         END; 
    $BODY$
    LANGUAGE plpgsql;

# public_category_after_insert

CREATE TRIGGER public_category_after_insert
 AFTER INSERT on public.category
   FOR EACH ROW EXECUTE PROCEDURE public_category_after_insert();

# Other handy syntax   
# DROP FUNCTION public_category_after_insert();
# DROP TRIGGER public_category_after_insert ON category;
