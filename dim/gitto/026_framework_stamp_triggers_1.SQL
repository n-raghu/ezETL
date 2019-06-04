CREATE OR REPLACE FUNCTION framework.get_timestamp() RETURNS TRIGGER 
LANGUAGE plpgsql
AS
$$
BEGIN
    NEW.last_mod_stamp=CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE TRIGGER t_jobs BEFORE UPDATE ON TABLE framework.jobs FOR EACH ROW EXECUTE PROCEDURE framework.get_timestamp();
CREATE OR REPLACE TRIGGER t_data_types BEFORE UPDATE ON TABLE framework.datatypes FOR EACH ROW EXECUTE PROCEDURE framework.get_timestamp();
CREATE OR REPLACE TRIGGER t_ins_config BEFORE UPDATE ON TABLE framework.instanceconfig FOR EACH ROW EXECUTE PROCEDURE framework.get_timestamp();
CREATE OR REPLACE TRIGGER t_collectionmaps BEFORE UPDATE ON TABLE framework.collectionmaps FOR EACH ROW EXECUTE PROCEDURE framework.get_timestamp();
CREATE OR REPLACE TRIGGER t_stakeholders BEFORE UPDATE ON TABLE framework.stakeholders FOR EACH ROW EXECUTE PROCEDURE framework.get_timestamp();
CREATE OR REPLACE TRIGGER t_api_sfdc_tracker BEFORE UPDATE ON TABLE framework.api_salesforce_tracker FOR EACH ROW EXECUTE PROCEDURE framework.get_timestamp();
CREATE OR REPLACE TRIGGER t_api_sfdc_mapper BEFORE UPDATE ON TABLE framework.api_salesforce_mappers FOR EACH ROW EXECUTE PROCEDURE framework.get_timestamp();
CREATE OR REPLACE TRIGGER t_statements BEFORE UPDATE ON TABLE framework.statements FOR EACH ROW EXECUTE PROCEDURE framework.get_timestamp();
