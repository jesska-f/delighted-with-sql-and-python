import delighted
import pandas as pd
from sqlalchemy 
import create_engine
import os

delighted.api_key = os.getenv('DELIGHTED_API_KEY')
DW_CONNECTION_STRING = os.getenv('DW_CONNECTION_STRING')
try:
    print("Info: Getting the list of people to send surveys to")
    engine = create_engine(DW_CONNECTION_STRING)
    query = """select   email, name, order_id, customer_id, status
               from magically_cleaned_data_for_customer_survey_sends
            """
    df =  pd.read_sql_query(query, engine)
except Exception as e:
    print("Error: Could not get list from DW")
    print("Error:",e)
try:
    for index, row in df.iterrows():        
        delighted.Person.create(email=row['email'],
                                name=row['name'],
                                properties={'customer_id':row['customer_id'],
                                            'order_id':row['order_id'],                                            
                                            'status':row['status']                                        
                                            })
        print("Info:",df.shape[0], "surveys sent")
except Exception as e:
    print("Error: Could not send surveys")
    print("Error:",e)
