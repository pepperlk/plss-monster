import os
import azure.functions as func
import logging
from arcgis_helpers import get_townshiplist, get_township
# from azure.storage.queue import QueueServiceClient, QueueMessage


app = func.FunctionApp()
# connstring = os.environ["AzureStore"]
# todo_queue= QueueServiceClient.from_connection_string(connstring).get_queue_client("todo")


@app.blob_trigger(arg_name="myblob", path="todo",
                               connection="AzureStore") 
def new_ts_blob(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")






# process on new queue item
@app.queue_trigger(arg_name="item", queue_name="todo",
                                 connection="AzureStore")
@app.queue_output(arg_name="output", queue_name="todo", connection="AzureStore")
def new_ts_queue(item: str, output: func.Out[func.QueueMessage]):
    logging.info(f"Python queue trigger function processed queue item"
                f"Queue Item: {item}")
    
    if item == "run":
        townships = get_townshiplist()

        townships = ['WY060360N0780W0', 'WY060370N0780W0'] 

        # for each townsship put in queue
        for township in townships:
            logging.info(f"Queueing township: {township}")

            output.set(township)

    else:
        logging.info(f"Unknown queue item: {item}")
        return
    




    



@app.route(route="get_township/{plssid:alpha}", trigger_arg_name="plssid", auth_level=func.AuthLevel.ANONYMOUS)
def http_get_township(req: func.HttpRequest, plssid: str) -> func.HttpResponse:
    logging.info("Python HTTP trigger function processed a request.")
    logging.info("Getting townships")
    townships = get_township(plssid)
    return func.HttpResponse(f"Townships for PLSSID {plssid}: {townships}", status_code=200)