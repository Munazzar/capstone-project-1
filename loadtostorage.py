import base64
from pandas import DataFrame
from json import loads
from google.cloud.storage import Client

class Loadtostorage:
     def __init__(self,event,context):
          self.event=event
          self.context=context
          self.bucket_name="capstone-project-1-342101-crypto-data-repo"

     def message_data(self):
          if "data" in self.event:
               pubsub_message = base64.b64decode(self.event['data']).decode('utf-8')
               return pubsub_message
          else:
               return ""
     def upload_data(self, df, filename="payload"):
          storage_client=Client()
          bucket=storage_client.bucket(self.bucket_name)
          blob=bucket.blob(f"{filename}.csv")
          blob.upload_from_string(data=df.to_csv(index=False), content_type="text/csv")
     
     def payload_to_df(self, message):
          try:
               df=DataFrame(loads(message))
               return df
          except Exception as e:
               raise

def process_request(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    
    print("Data received")
    lts=Loadtostorage(event,context)
    message=lts.message_data()
    upload_df=lts.payload_to_df(message)
    time=upload_df['price_timestamp'].unique().tolist()[0]
    lts.upload_data(upload_df, "crypto_data"+str(time))

