"""Per-entity silver column projection.

Bronze pulls EVERY column from Salesforce ("拉取全部列"); silver keeps only the
"指定列" and drops dlt system columns (_dlt_load_id, _dlt_id). The column lists
below are seeded from the original hand-tuned SELECTs in sf.py `QUERIES`
(keeping the "参考我原来的逻辑" promise), then EXTENDED with the extra fields the
gold aggregations (ports of daily.py / monthly_test.py) reference — bronze
already has them, so extending the silver projection is free.

Keys are the bronze table names (Salesforce entity API name lower-cased, the
name dlt writes). Values are the business columns to publish. The primary key
(`Id`), the incremental cursor (SystemModstamp / CreatedDate) and the soft-delete
flag (`IsDeleted`) are NOT listed here — they are technical columns handled by
the silver_dedupe macro (used for ranking / filtering) and intentionally dropped
from the published set. List `Id` is added automatically by the generator.

To publish a new column: add it to the entity's list here and re-run
gen_silver.py. To add a whole new entity: add its API name to
sf_bronze.ENTITIES and an entry here.
"""

# Fields appended to sf.py's original SELECTs because the gold models need them.
_GOLD_EXTRA = {
    "certificate_c__c": [
        "RecordTypeId",
        "Boreal_Forests__c",
        "Community_Forestry__c",
        "Plantations__c",
        "Temperate_Forests__c",
        "Tropical_Forests__c",
        "Management_for_NTFP_and_Services__c",
        "Trader_Forest_Products_Turnover_USD__c",
        "Processor_Forest_Products_Turnover_USD__c",
        "Reason_for_Termination__c",
    ],
    "account": [
        "RecordTypeId",
        "Sub_Chamber__c",
        "Member_Type__c",
        "Chamber__c",
    ],
    "country_data__c": ["Hemisphere__c"],
}

# Published columns per bronze table (Id is prepended automatically; IsDeleted /
# cursor are dropped). Derived from sf.py QUERIES minus IsDeleted & cursor.
SILVER_COLUMNS = {
    "product_species__c": [
        "Genus__c", "Product_Class__c", "Species__c", "CreatedDate",
    ],
    "product_classification__c": [
        "Name", "Certificate__c", "Main_Output_Category__c", "Primary_Activity__c",
        "Secondary_Activity__c", "Trade_Name__c", "Level_1__c", "Level_2__c",
        "Level_3__c", "Regulatory_Module__c", "CreatedDate",
    ],
    "account": [
        "Name", "Type", "Website", "Certificate_Relation__c", "Country__c",
        "Date_To__c", "Hide_Site__c", "State_County__c", "Street__c",
        "Town_City__c", "Zip_Postal_Code__c", "Local_Company_Name__c",
        "FSC_Site_subcode__c", "Trade_Name__c", "Registration_Number__c",
        "Mailing_Address_Country__c", "Mailing_State_County__c",
        "Mailing_Town_City__c", "Mailing_Zip_Postal_Code__c", "Mailing_Street__c",
    ],
    "certificate_c__c": [
        "Name", "CB__c", "Cert_Status__c", "Certificate_Number__c",
        "Certificate_Type__c", "Controlled_Wood_Code__c", "Controlled_Wood__c",
        "Date_From__c", "Date_To__c", "Forest_Area_Total__c", "Forest_Type__c",
        "Forest_Zone__c", "Full_Certificate_Code__c", "Number_of_Sites__c",
        "Standard__c", "First_Issue_Date__c", "Date_of_Suspension__c",
        "Former_Cert_Code__c", "System_of_Control__c", "License_Status__c",
        "CW_Due_Dilligence__c", "Certification_Statement__c", "Process_Activity__c",
    ],
    "certificate_status__c": [
        "Certificate__c", "Status__c", "Date_From__c", "Date_of_Suspension__c",
        "Date_To__c",
    ],
    "certificate_attachment__c": [
        "Certificate__c", "Active__c", "Document_Type__c", "Subject__c",
    ],
    "contact": [
        "AccountId", "Name", "Phone", "Email", "HasOptedOutOfEmail",
        "Public_Contact__c", "Secondary_e_mail__c", "Portal_Enabled__c",
        "Portal_Username__c", "Primary_Company_Contact_for_FSC__c",
    ],
    "non_certificate_holder__c": [
        "Name", "Organization__c", "Contract_start_date__c", "Contract_end_date__c",
        "License_Status__c", "policy_omit_trademark_symbols__c",
    ],
    "country_data__c": [
        "Continent__c", "Country_code__c", "ISO_Code__c", "Name",
    ],
    "recordtype": [
        "Name",
    ],
    "species__c": [
        "Name", "OwnerId",
    ],
    "evaluation__c": [
        "Contact_Email__c", "Date_From__c", "Date_To__c", "Schedule_Evaluation__c",
        "Evaluation_Type__c", "Forest_Type__c", "Forest_Area__c",
        "Display_publicly__c", "Company__c",
    ],
    "project_certificate__c": [
        "Name", "Application_Date__c", "CB__c", "Certificate_Status__c",
        "Certification_Date__c", "Certification_Type__c", "Full_Code__c",
        "Organization__c", "Project_Country__c", "Project_Name__c",
        "Project_Scope__c", "Project_Street__c", "Project_Town_City__c",
        "Standard__c",
    ],
    "sales_history__c": [
        "Audit_Period_From__c", "Certificate__c", "Sales_activity_Status__c",
        "Sales_Activity_To__c",
    ],
    "transaction_verification_findings__c": [
        "CAR__c", "Certificate_Relation__c", "Description_Notes__c",
        "Effective_Date__c", "Finding__c", "Name", "Participating_Site__c",
    ],
}


def published_columns(bronze_table):
    """Final ordered column list for a silver model: Id + configured + gold extras.

    Returns None for tables with no config (generator falls back to keep-all).
    De-duplicates while preserving order in case an extra already appears above.
    """
    base = SILVER_COLUMNS.get(bronze_table)
    if base is None:
        return None
    cols, seen = [], set()
    for c in ["Id", *base, *_GOLD_EXTRA.get(bronze_table, [])]:
        if c not in seen:
            cols.append(c)
            seen.add(c)
    return cols
