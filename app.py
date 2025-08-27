from flask import Flask, render_template, request, redirect, url_for
from anamoly_detector import run_anomaly_check
from apscheduler.schedulers.background import BackgroundScheduler
import pandas as pd
import os
import atexit

app = Flask(__name__)

# Home route: Show anomaly table and current status
@app.route('/')
def home():
    # Read anomaly log file
    try:
        df = pd.read_csv("logs/anomalies_log.csv")
    except FileNotFoundError:
        df = pd.DataFrame(columns=["No", "anomalies yet", "detected"])

    # Read status message
    try:
        with open("logs/status.txt", "r", encoding="utf-8") as f:
            message = f.read()
    except FileNotFoundError:
        message = "Status not available."

    return render_template("index.html", table=df.to_html(classes='table table-bordered', index=False), message=message)

# Manual check button: POST request from UI
@app.route('/manual-check', methods=['POST'])
def manual_check():
    run_anomaly_check()
    return redirect(url_for('home'))  # 🚀 Redirect to home after POST to prevent 405 error on refresh

# Background task to run every 5 minutes
def scheduled_anomaly_check():
    print("[Scheduler] Running automatic anomaly check...")
    run_anomaly_check()

# Setup APScheduler
scheduler = BackgroundScheduler()
scheduler.add_job(scheduled_anomaly_check, 'interval', minutes=5)
scheduler.start()

# Shutdown scheduler cleanly on exit
atexit.register(lambda: scheduler.shutdown())

# Run the app
if __name__ == '__main__':
    app.run(debug=True, use_reloader=False)

