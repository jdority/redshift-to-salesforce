#
# Heroku Postgres Schemas:
#    * public - staging schema where AWS GLUE writes the Redshift rows for the category table.
#    * salesforce - this is the schema that Heroku Connect uses.
# 
# After AWS Glue writes a row to the public staging schema, this function will upsert the row into the salesforce schema
# so Heroku Connect can then sync to salesforce;
#
# public_category_after_insert()

CREATE OR REPLACE FUNCTION public_category_after_insert()
    RETURNS trigger AS
    $BODY$
    BEGIN
        UPDATE salesforce.category__c
           SET (catgroup__c, name, catdesc__c) = (NEW.catgroup, NEW.catname, NEW.catdesc)
         WHERE catid__c = NEW.catid; 
        
        IF NOT FOUND THEN 
           INSERT INTO salesforce.category__c (catid__c, catgroup__c, name, catdesc__c) 
                                       VALUES (NEW.catid, NEW.catgroup, NEW.name, NEW.catdesc);
        END IF; 
        RETURN NULL;
    END; 
    $BODY$
    LANGUAGE plpgsql;

# public_category_after_insert

    CREATE TRIGGER public_category_after_insert
       AFTER INSERT on public.category
    FOR EACH ROW EXECUTE PROCEDURE public_category_after_insert();
    
# DROP FUNCTION public_category_after_insert();
# DROP TRIGGER public_category_after_insert ON category;
