import pandas as pd
import csv
from datetime import datetime
import yagmail
import os

# 🔧 File paths
DATA_PATH = "data/SampleSuperstore.csv"
REPORT_PATH = "logs/anomalies_log.csv"
LOG_PATH = "logs/system_log.csv"
STATUS_PATH = "logs/status.txt"

# 📧 Email config
SENDER_EMAIL = "hitheshap@gmail.com"
RECEIVER_EMAIL = "shreypersonal1@gmail.com"
APP_PASSWORD = "kxok utpd pqxe yayx"  # Consider moving this to an environment variable

# 🎯 Thresholds
DISCOUNT_THRESHOLD = 0.5

def detect_anomalies():
    df = pd.read_csv(DATA_PATH, encoding="ISO-8859-1")
    anomalies = df[
        (df['Quantity'] <= 0) |
        (df['Sales'] <= 0) |
        (df['Discount'] < 0) |
        (df['Discount'] > DISCOUNT_THRESHOLD) |
        (df['Customer ID'].isna())
    ]
    return anomalies

def log_anomaly(message, level="Warning"):
    with open(LOG_PATH, mode="a", newline='', encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow([datetime.now().strftime("%Y-%m-%d %H:%M:%S"), message, level])

def send_email(total, cols, attach_file):
    body = f"""
    Hello,

    🚨 Anomalies Detected!

    - Total: {total}
    - Columns Affected: {', '.join(cols) if cols else 'Unknown'}

    Please find the attached anomaly log.

    Regards,
    Data Quality Bot
    """
    yag = yagmail.SMTP(user=SENDER_EMAIL, password=APP_PASSWORD)
    yag.send(to=RECEIVER_EMAIL, subject="🚨 Data Quality Alert", contents=body, attachments=attach_file)

def analyze_columns(anomalies):
    affected = []
    if (anomalies['Quantity'] <= 0).any(): affected.append('Quantity')
    if (anomalies['Sales'] <= 0).any(): affected.append('Sales')
    if (anomalies['Discount'] < 0).any() or (anomalies['Discount'] > DISCOUNT_THRESHOLD).any(): affected.append('Discount')
    if anomalies['Customer ID'].isna().any(): affected.append('Customer ID')
    return affected

def run_anomaly_check():
    try:
        anomalies = detect_anomalies()

        if anomalies.empty:
            status = "✅ No anomalies found during the last check."
            log_anomaly(status, "Info")
        else:
            # Check for previously reported anomalies
            if os.path.exists(REPORT_PATH):
                prev_anomalies = pd.read_csv(REPORT_PATH)
                if anomalies.equals(prev_anomalies):
                    status = "ℹ️ Anomalies found but already reported earlier. No new email sent."
                    log_anomaly("Repeated anomalies detected. Email not resent.", "Info")
                else:
                    anomalies.to_csv(REPORT_PATH, index=False)
                    affected = analyze_columns(anomalies)
                    send_email(len(anomalies), affected, REPORT_PATH)
                    status = f" {len(anomalies)} new anomalies found and email sent."
                    log_anomaly(status, "Critical")
            else:
                anomalies.to_csv(REPORT_PATH, index=False)
                affected = analyze_columns(anomalies)
                send_email(len(anomalies), affected, REPORT_PATH)
                status = f" {len(anomalies)} anomalies detected and email sent (first report)."
                log_anomaly(status, "Critical")

        # Save status message for Flask to display
        with open(STATUS_PATH, "w", encoding="utf-8") as f:
            f.write(status)

        return status

    except Exception as e:
        error_message = f" Error during anomaly check: {str(e)}"
        log_anomaly(error_message, "Error")
        with open(STATUS_PATH, "w", encoding="utf-8") as f:
            f.write(error_message)
        return error_message
