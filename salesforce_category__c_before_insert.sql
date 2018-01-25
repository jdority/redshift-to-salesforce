#
# Heroku Postgres Schemas:
#    * public - staging schema where AWS GLUE writes the Redshift rows for the category table.
#    * salesforce - this is the schema that Heroku Connect uses.
# 
# After AWS Glue writes a row to the public staging schema, this function will upsert the row into the salesforce schema
# so Heroku Connect can then sync to salesforce;
#
# ***** Must provide a CONSTRAINT - Unique Index alone will produce an error
# ALTER TABLE salesforce.category__c ADD CONSTRAINT unique_salesforce_category__c UNIQUE USING INDEX hcu_idx_category__c_catid__c;
#
CREATE OR REPLACE FUNCTION salesforce_category__c_before_insert()
    RETURNS trigger AS
    $BODY$
    BEGIN
        INSERT INTO salesforce.category__c (catid__c, catgroup__c, name, catdesc__c)
                                    VALUES (NEW.catid__c, NEW.catgroup__c, NEW.name, NEW.catdesc__c)
               ON CONFLICT ON CONSTRAINT unique_salesforce_category__c
               DO UPDATE SET (catgroup__c, name, catdesc__c) = (NEW.catgroup__c, NEW.name, NEW.catdesc__c);
        RETURN NULL;
    END;
    $BODY$
    LANGUAGE plpgsql;

CREATE TRIGGER salesforce_category__c_before_insert
       BEFORE INSERT ON salesforce.category__c
          FOR EACH ROW EXECUTE PROCEDURE salesforce_category__c_before_insert();
    
# DROP FUNCTION salesforce_category__c_before_insert();

# *** Using Heroku CLI, run the following command to get the correct name of the trigger \d salesforce.category__c;      
# DROP TRIGGER hc_category__c_status_trigger on salesforce.category__c;
