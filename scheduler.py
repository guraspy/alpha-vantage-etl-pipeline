import schedule
import time
from stock_etl import run_etl_pipeline

# schedule the job to run every day at a specific time
schedule.every().day.at("00:30").do(run_etl_pipeline)

print("Scheduler started. Waiting for the scheduled time")

while True:
    schedule.run_pending()
    time.sleep(60) # check every minute to dont miss specified time