#!/usr/bin/env python
# coding: utf-8

# In[7]:


# Job Scheduler

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import subprocess


def run_AutoTA1():
    subprocess.call(["python", "./jobtest.py"])

def run_AutoTA2():
    subprocess.call(["python", "./jobtest.py"])

def run_AutoTA3():
    subprocess.call(["python", "./jobtest.py"])

scheduler = BackgroundScheduler()

# Run the job hourly during work hours. Adjusted for UTC (+5 hours) 
scheduler.add_job(run_AutoTA1, trigger=CronTrigger(day_of_week = 'mon-fri', hour = '13-22', minute = '30', second = '0'))  # , args=["1"]))   */10... runs every 10 min, etc.
#scheduler.add_job(run_AutoTA2, trigger=CronTrigger(day_of_week = 'mon-fri', hour = '13-22', minute = '2', second = '0'))
#scheduler.add_job(run_AutoTA3, trigger=CronTrigger(day_of_week = 'mon-fri', hour = '13-22', minute = '4', second = '0'))

#scheduler.add_job(run_my_script, trigger=CronTrigger(day_of_week = 'mon-fri', hour = '12-21', minute = '0', second = '0')) # For daylight savings

scheduler.start()

# Keep the main thread alive (makes BackgroundScheduler into a BlockScheduler so output can be seen) 
#try:
#    while True:
#        pass
#except (KeyboardInterrupt, SystemExit):
#    scheduler.shutdown()


# In[ ]:




