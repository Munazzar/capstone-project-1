from requests import Session
from time import sleep
import logging

from concurrent import futures
from google.cloud.pubsub_v1 import PublisherClient
from google.cloud.pubsub_v1.publisher.futures import Future


class publish_data:
    def __init__(self):
        self.project_id="capstone-project-1-342101"
        self.topic_id="capstone-project-1-342101-crpto-pull"
        self.publisher_client=PublisherClient()
        self.topic_path=self.publisher_client.topic_path(self.project_id,self.topic_id)
        self.publish_futures = []

    def get_data_from_api(self) -> str:

        params = {
        "key": "d24219b11cb0c80e460b5ae9e1a8850a77e95303",
        "conver": "USD",
        "interval":"1d",
        "per-page":"100",
        "page":"1",

        }
        ses=Session()
        res=ses.get("https://api.nomics.com/v1/currencies/ticker", params=params, stream=True)

        if 400> res.status_code>=200:
            logging.info(f"response : {res.status_code} :- {res.text}")
            # print(f"response : {res.status_code} :- {res.text}")
            return res.text
        else:
            raise Exception(f"failed to fetch data: {res.status_code}: {res.text}")
        
    def get_callback(self,publish_future: Future,data )->callable:
        def callback(publish_future):
            try:
                logging.info(publish_future.result(timeout=60))
            except futures.TimeoutError:
                logging.error("publishing data timed out")
        return callback
    def publish_data_pub(self, data) -> None:
        
        publish_future=self.publisher_client.publish(self.topic_path, data.encode("utf-8"))
        print("printing")
        publish_future.add_done_callback(self.get_callback(publish_future,data))
        self.publish_futures.append(publish_future)
        futures.wait(self.publish_futures,return_when=futures.ALL_COMPLETED)


if __name__ == "__main__":
    # init_logging()
    pd=publish_data()
    for i in range(10):
        data=pd.get_data_from_api()
        pd.publish_data_pub(data)
        sleep(90)



