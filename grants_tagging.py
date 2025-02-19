import ast
import json
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Dict, List, Optional
from bson import ObjectId
from dotenv import load_dotenv
from langchain.document_loaders.mongodb import MongodbLoader
import uuid
import hashlib
import json
import boto3
from pymongo import MongoClient
from langchain_aws import ChatBedrock



load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DATABASE_NAME = os.getenv("MONGODB_DATABASE_NAME")


#Bedrock

chat = ChatBedrock(
    model_id="anthropic.claude-3-sonnet-20240229-v1:0",
    region_name="us-east-1",
    model_kwargs={"temperature": 0.1},
)


MONGO_DB_COLLECTION = "all_opportunities"




def setup_logging():
    logging.basicConfig(level=logging.INFO)
    logging.info("Setting up logging...")


def sanitize_json_string_quotes(s):
    logging.info("🔄 Starting to replace single quotes with double quotes...")
    pattern = r"""(?<!\\)'((?:[^'\\]|\\.)*)'(?![\\'])"""
    s = re.sub(pattern, lambda m: '"' + m.group(1).replace("\\'", "'") + '"', s)
    logging.info("✅ Replacement done.")
    return s


def load_environment_variables():
    logging.info("Loading environment variables...")
    load_dotenv()
    env_vars = {
        
        "MONGODB_URI": os.getenv("MONGODB_URI"),
        "MONGODB_DATABASE_NAME": os.getenv("MONGODB_DATABASE_NAME"),
        
    }
    logging.info("🌍 Environment variables loaded.")
    return env_vars



def initialize_clients(env_vars: dict):
    logging.info("Initializing clients...")
   

    loader = MongodbLoader(
        connection_string=env_vars["MONGODB_URI"],
        db_name=env_vars["MONGODB_DATABASE_NAME"],
        collection_name=MONGO_DB_COLLECTION,
    )
    logging.info("Clients initialized.")
    return  loader




def process_document_with_uuid(doc, metadata_keys: Dict[str, str]) -> Optional[dict]:
    logging.info("Processing a document with UUID...")

    try:
        # Assign a UUID as the document ID
        doc_id = str(uuid.uuid4())
        cleaned_content = re.sub(r"ObjectId\('\w+'\)", "''", doc.page_content)
        page_content_dict = ast.literal_eval(cleaned_content)
        
        selected_data = {
            "id": page_content_dict.get("id"),
            "opportunityTitle": page_content_dict.get("opportunityTitle"),
            "synopsis": page_content_dict.get("synopsis"),
            "opportunityPkgs": page_content_dict.get("opportunityPkgs")
        }

       
        
        doc.metadata = metadata_keys
        doc.page_content = json.dumps(selected_data)

        # Set the UUID to the metadata
        doc.metadata['document_id'] = doc_id
        
        return doc
    except Exception as e:
        logging.error(f"Failed to parse document content. Error: {e}")
        return None



def process_documents_in_batch(docs: List[dict], batch_size: int) -> List[dict]:
    logging.info("Processing documents in batches...")
    for i in range(0, len(docs), batch_size):
        yield docs[i : i + batch_size]
    logging.info("Done processing documents in batches.")


def tagg_grants(grants_data):
    """Processes grants data using Anthropic's Claude and returns a JSON output.

    Args:
        grants_data: A list of grant descriptions.

    Returns:
        A JSON string containing the processed grant data.
    """

    system_prompt = """You are given a document containing grants information as context. Use this context to perform the following tasks:

    Grants: {context}

    For every grant: 
     Task 0: Get me the ID [OpportunityId]
     Task 1: Extract Research Type Tags
     Identify and categorize each grant based on the type of research it supports. Assign one or more of the following tags to each grant based on the descriptions provided:

     - Clinical: Grants supporting research involving direct clinical trials on humans.
     - Non-Clinical: Grants focused on theoretical research, technology development, or other research activities that do not involve preclinical or clinical studies.

     Task 2: Extract SBIR Tags
     Determine the SBIR tags of company or organization that each grant targets. Use the descriptions within the grants to assign one of the following tags:

     - SBIR: Small Business Innovation Research program grants targeted at small businesses.
     - STTR: Small Business Technology Transfer program grants designed to facilitate cooperation between small businesses and research institutions.
     - SBIR/STTR: Grants that are part of both SBIR and STTR programs.
     - Non-SBIR/STTR: Grants that are not part of the SBIR or STTR programs.

     Task 3: 
     Identify the company type tags of each grant.
     - Academic
     - For Profit
     - Non Profit
     Task 4:
     Identify the country-based eligibility.
     Review the grant documentation to ascertain the geographic eligibility of applicants. Specify which countries or regions are allowed to apply, based on the legal registration and operational mandates mentioned in the grant.

     Task 5:
     Identify the country operation eligibility.
     Determine and list the countries in which the funded activities can be conducted. Check the grant details for any mention of specific geographic limitations or preferences regarding where the grant-funded projects or operations can take place.
    """

    user_prompt = """Return your output in a JSON format. No explanation required.
                    Example Output: {"id":"string", "researchTypeTags":["string"],"sbirTags":["string"],"companyTypeTags":["string"],"countryBasedEligibility":["string"],"countryOperationEligibility":["string"]}
                  """

    results = []
    #for grant in grants_data:
        # Use chat.invoke to interact with the chat model
    response = chat.predict(f"{system_prompt.format(context=grants_data)} {user_prompt}")
    # print(response, type(response))
    results.append(json.loads(response))

    return json.dumps(results)

def update_mongodb(processed_grants):
        
        """Updates MongoDB with the processed grants data.

        Args:
            processed_grants: A JSON string containing processed grant data.
        """
        client = None  # Assign client to None initially
        try:
            client = MongoClient(MONGODB_URI)  
            db = client[MONGODB_DATABASE_NAME]   
            collection = db['all_opportunities']  
         

            grants = json.loads(processed_grants)
            
            for grant in grants:
               
                collection.update_one({'id': grant['id']}, {'$set': grant}, upsert=False)
                

            print("MongoDB updated successfully!")

        except Exception as e:
            print(f"Error updating MongoDB: {e}")

        finally:
            if client:  # Now client is always defined
                client.close()




def main():
    setup_logging()
    env_vars = load_environment_variables()
    loader = initialize_clients(env_vars)

    logging.info("Loading documents...")

    documents = loader.load()
    # documents = documents[0]
    # logging.info(f"📄 First document: {documents[0]}")
    assert documents is not None, "Failed to load documents."
    
    logging.info(f"Loaded {len(documents)} documents")

    metadata_keys = {
        "database" : "freemind2",
        "collection" : "all_opportunities",
    }

    processed_documents = [process_document_with_uuid(doc, metadata_keys) for doc in documents]
    processed_documents = [doc for doc in processed_documents if doc is not None]
    logging.info(f"Number of processed documents: {len(processed_documents)}")
    batch_size = 50
    start_time = time.time()
    count = 0
    

    
    with ThreadPoolExecutor() as executor:
        for i, batch in enumerate(process_documents_in_batch(processed_documents, batch_size)):
            tagging = list(executor.map(tagg_grants, batch))
            executor.map(update_mongodb, tagging) 

            logging.info(f"Generated tag: {tagging}")



if __name__ == "__main__":
    main()
    
