from dotenv import load_dotenv, find_dotenv
import os 
import pprint
from pymongo import MongoClient

load_dotenv(find_dotenv())

password = os.environ.get("MONGODB_PWD")
connection_string = f"mongodb+srv://alexflibustier:{password}@mycluster.2lzwc5x.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(connection_string, authSource="admin")

dbs = client.list_database_names()
test_db = client.covid19
collections = test_db.list_collection_names()
# print(collections)


#INSERTING DOCUMENTS
def insert_doc():
    collections = test_db.covid19
    # create file to insert into collection
    test_document = {
        "name":"Alex",
        "type":"Test"
    }
    inserted_id = collections.insert_one(test_document).inserted_id
    print(inserted_id)


covid_19 = client.covid_19
covid_collection = covid_19.covid_collection

def creating_documents():
    table_names = ["CT_US_COVID_TESTS", 
                   "DATABANK_DEMOGRAPHICS",
                   "GOOG_GLOBAL_MOBILITY_REPORT",
                   "HDX_ACAPS",
                   "HS_BULK_DATA",
                   "HUM_RESTRICTIONS_AIRLINE",
                   "HUM_RESTRICTIONS_COUNTRY",
                   "IHME_COVID_19",
                   "JHU_COVID_19",
                   "KFF_HCP_CAPACITY",
                   "KFF_US_ICU_BEDS",
                   "KFF_US_POLICY_ACTIONS",
                   "KFF_US_STATE_MITIGATIONS",
                   "METADATA",
                   "NYC_HEALTH_TESTS",
                   "NYT_US_COVID19",
                   "PCM_DPS_COVID19",
                   "PCM_DPS_COVID19_DETAILS",
                   "RKI_GER_COVID19_DASHBOARD",
                   "SCS_BE_DETAILED_PROVINCE_CASE_COUNTS",
                   "SCS_BE_DETAILED_HOSPITALISATIONS",
                   "SCS_BE_DETAILED_MORTALITY",
                   "SCS_BE_DETAILED_TESTS",
                   "WHO_SITUATION_REPORTS"]

    tables_full_name = ["US COVID-19 testing and mortality", 
                          "Global demographic data",
                          "Global mobility data",
                          "ACAPS public health restriction data",
                          "Global data on healthcare providers",
                          "Travel restrictions by airline",
                          "Travel restrictions by country",
                          "Forecasts from IHME",
                          "Global case counts",
                          "US healthcare capacity by state, 2018",
                          "ICU beds by county, US",
                          "US policy actions by state",
                          "US actions to mitigate spread, by state",
                          "Table metadata",
                          "Detailed data on New York City",
                          "US case and mortality counts, by county",
                          "Italy case statistics, summary",
                          "Italy case statistics, detailed",
                          "Detailed case counts and mortality by districts (Kreise), Germany",
                          "Detailed case counts by province, sex and age band, Belgium",
                          "Detailed hospitalisations by type of hospital care, Belgium",
                          "Detailed mortality by region, sex and age band, Belgium",
                          "Number of tests performed by day, Belgium",
                          "WHO situation reports"
                          ]

    sources = ['https://covidtracking.com/', 
             "https://data.worldbank.org/indicator/sp.pop.totl",
             "https://www.google.com/covid19/mobility/",
             "https://data.humdata.org/dataset/acaps-covid19-government-measures-dataset",
             "OpenStreetMap, via: https://healthsites.io/",
             "https://data.humdata.org/dataset/covid-19-global-travel-restrictions-and-airline-information",
             "https://data.humdata.org/dataset/covid-19-global-travel-restrictions-and-airline-information",
             "http://www.healthdata.org/covid/data-downloads",
             "https://github.com/CSSEGISandData/COVID-19",
             "https://www.kff.org/health-costs/issue-brief/state-data-and-policy-actions-to-address-coronavirus/",
             "https://www.kff.org/health-costs/issue-brief/state-data-and-policy-actions-to-address-coronavirus/",
             "https://www.kff.org/health-costs/issue-brief/state-data-and-policy-actions-to-address-coronavirus/",
             "https://www.kff.org/health-costs/issue-brief/state-data-and-policy-actions-to-address-coronavirus/",
             "Sorry, for table 'METADATA' link does not exist.",
             "https://github.com/nychealth/coronavirus-data",
             "https://github.com/nytimes/covid-19-data",
             "https://github.com/pcm-dpc/COVID-19",
             "https://github.com/pcm-dpc/COVID-19",
             "https://experience.arcgis.com/experience/478220a4c454480e823b17327b2bf1d4/page/page_1/",
             "https://www.sciensano.be/en",
             "https://www.sciensano.be/en",
             "https://www.sciensano.be/en",
             "https://www.sciensano.be/en",
             "https://www.who.int/emergencies/diseases/novel-coronavirus-2019/situation-reports"
             ]

    download_links = ["https://s3-us-west-1.amazonaws.com/starschema.covid/CT_US_COVID_TESTS.csv", 
                "https://s3-us-west-1.amazonaws.com/starschema.covid/DATABANK_DEMOGRAPHICS.csv",
                "https://s3-us-west-1.amazonaws.com/starschema.covid/GOOG_GLOBAL_MOBILITY_REPORT.csv",
                "https://s3-us-west-1.amazonaws.com/starschema.covid/HDX_ACAPS.csv",
                "https://s3-us-west-1.amazonaws.com/starschema.covid/HS_BULK_DATA.csv",
                "https://s3-us-west-1.amazonaws.com/starschema.covid/HUM_RESTRICTIONS_AIRLINE.csv",
                "https://s3-us-west-1.amazonaws.com/starschema.covid/HUM_RESTRICTIONS_COUNTRY.csv",
                "https://s3-us-west-1.amazonaws.com/starschema.covid/IHME_COVID_19.csv",
                "https://s3-us-west-1.amazonaws.com/starschema.covid/JHU_COVID-19.csv",
                "https://s3-us-west-1.amazonaws.com/starschema.covid/KFF_HCP_CAPACITY.csv",
                "https://s3-us-west-1.amazonaws.com/starschema.covid/KFF_US_ICU_BEDS.csv",
                "https://s3-us-west-1.amazonaws.com/starschema.covid/KFF_US_POLICY_ACTIONS.csv",
                "https://s3-us-west-1.amazonaws.com/starschema.covid/KFF_US_STATE_MITIGATIONS.csv",
                "https://s3-us-west-1.amazonaws.com/starschema.covid/METADATA.csv",
                "https://s3-us-west-1.amazonaws.com/starschema.covid/NYC_HEALTH_TESTS.csv",
                "https://s3-us-west-1.amazonaws.com/starschema.covid/NYT_US_COVID19.csv",
                "https://s3-us-west-1.amazonaws.com/starschema.covid/PCM_DPS_COVID19.csv",
                "https://s3-us-west-1.amazonaws.com/starschema.covid/PCM_DPS_COVID19_DETAILS.csv",
                "https://s3-us-west-1.amazonaws.com/starschema.covid/RKI_GER_COVID19_DASHBOARD.csv",
                "https://s3-us-west-1.amazonaws.com/starschema.covid/SCS_BE_DETAILED_PROVINCE_CASE_COUNTS.csv",
                "https://s3-us-west-1.amazonaws.com/starschema.covid/SCS_BE_DETAILED_HOSPITALISATIONS.csv",
                "https://s3-us-west-1.amazonaws.com/starschema.covid/SCS_BE_DETAILED_MORTALITY.csv",
                "https://s3-us-west-1.amazonaws.com/starschema.covid/SCS_BE_DETAILED_TESTS.csv",
                "https://s3-us-west-1.amazonaws.com/starschema.covid/WHO_SITUATION_REPORTS.csv"
                ]
    
    docs = []

    for table_name, table_full_name, source, download_link in zip(table_names, tables_full_name, sources, download_links):
        doc = {
            "table_name": table_name, 
            "utilities": {
                "table_full_name": table_full_name, 
                "source":source,
                "download_link":download_link
                }
            }
        
        docs.append(doc)
        
    covid_collection.insert_many(docs)
    
# creating_documents()




# READING DOCUMENTS

printer = pprint.PrettyPrinter()

def find_all_tables():
    tables = covid_collection.find()

    for table in tables:
        printer.pprint(table)

# find_all_tables()


def find_table():
    table = covid_collection.find_one({'table_name': 'SCS_BE_DETAILED_MORTALITY'})
    printer.pprint(table)

# find_table()


def find_table_by_id(table_id):
    from bson.objectid import ObjectId

    _id = ObjectId(table_id)
    table = covid_collection.find_one({"_id": _id})
    printer.pprint(table)

# def get_something(num1, num2):
#     query = {"$and": [
#             {"source": {"$gte": num1}},
#             {"source": {"$lte": num2}}
#             ]}
#     tables = covid_collection.find(query).sort("...")
#     for table in tables:
#         printer.pprint(table)


def project_columns():
    columns = {"_id": 0, "table_name": 1, "utilities.source": 1, "utilities.table_full_name": 1}
    tables = covid_collection.find({}, columns)
    for table in tables:
        printer.pprint(table)

# project_columns()


# UPDATING DATA
def update_table_by_id(table_id):
    from bson.objectid import ObjectId

    _id = ObjectId(table_id)

    # UPDATE
    # all_updates = {
    #     "$set": {"new_field": True},  # if the field exist, the 'set' will overwrite it
    #     #"$rename": {"utilities.source": "source_link"}, # rename the 'name' of the field. Not the value
    # }
    # covid_collection.update_one({"_id": _id}, all_updates)

    # DELETE
    covid_collection.update_one({"_id": _id}, {"$unset": {"new_field": ""}})

update_table_by_id('64ea0ce0408e90c4e75f1a76')
find_table_by_id('64ea0ce0408e90c4e75f1a76')
    