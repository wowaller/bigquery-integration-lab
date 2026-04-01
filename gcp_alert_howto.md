# How-To Guide: Cloud Composer Alerting with Webhooks

This guide explains how to set up alerting for Google Cloud Composer (Airflow) to send structured JSON payloads to an external HTTP webhook. 

Depending on your requirements for customization and decoupling, you can choose between two primary approaches:

---

## Approach 1: Native Airflow `on_failure_callback` (Custom JSON Schema)

Use this approach if you need **full control** over the JSON output structure (e.g., specific keys like `dag_id`, `task_id`, `duration`).

### How It Works
You define a Python function that uses Python’s `requests` library to send a POST request directly to the webhook when a task fails.

### Step-by-Step Instructions

1.  **Define the Callback in your DAG File**
    Add the `on_failure_callback` function to your DAG file (similar to the example in `run_dataform_job.py`).

    ```python
    import requests
    from datetime import datetime

    def failure_webhook_callback(context):
        ti = context.get('task_instance')
        
        payload = {
            "task_name": ti.task_id if ti else "N/A",
            "task_id": ti.task_id if ti else "N/A",
            "dag_id": ti.dag_id if ti else "N/A",
            "task_status": ti.state if ti else "failed",
            "duration": ti.duration if ti and ti.duration else None,
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }

        # Replace with your customer webhook URL
        WEBHOOK_URL = "https://your-customer-endpoint.com/api"

        try:
            requests.post(WEBHOOK_URL, json=payload, timeout=10)
        except Exception as e:
            print(f"Failed to send webhook alert: {e}")
    ```

2.  **Attach to DAG**
    Add `on_failure_callback=failure_webhook_callback` to your `DAG` instantiation:
    ```python
    with DAG(
        dag_id="your_dag_id",
        ...
        on_failure_callback=failure_webhook_callback,
    ) as dag:
    ```

---

## Approach 2: GCP Log-Based Alerting + Webhook Channel (Fully Decoupled)

Use this approach if you want a pure infrastructure-level solution. This method requires no code modifications to your Airflow tasks and is managed by GCP.

### How It Works
A log-based alert triggers when failure logs appear in Cloud Logging. Google routes the alert to your webhook via Cloud Monitoring.

### Step-by-Step Instructions

#### 1. Create a Notification Channel
You must first create a notification channel that points Cloud Monitoring to your webhook URL.

**Using Gcloud CLI:**
Create a file `webhook_channel.json`:
```json
{
  "type": "webhook_tokenauth",
  "displayName": "Customer Webhook Channel",
  "labels": {
    "url": "https://your-customer-endpoint.com/webhook"
  }
}
```
Run the command:
```bash
gcloud beta monitoring channels create --channel-content-from-file=webhook_channel.json
```

---

#### 2. Create an Alerting Policy
You then define a policy that triggers on specific logs and routes to that channel.

**Define Policy in `log_alert_policy.json`:**
```json
{
  "displayName": "Task Failure Alert",
  "enabled": true,
  "combiner": "OR",
  "conditions": [
    {
      "displayName": "Task Failure Conditions",
      "conditionMatchedLog": {
        "filter": "resource.type=\"cloud_composer_environment\" AND textPayload:\"Task failed\""
      }
    }
  ],
  "alertStrategy": {
    "notificationRateLimit": {
      "period": "300s"
    },
    "autoClose": "1800s"
  }
}
```

**Apply using Gcloud CLI:**
```bash
gcloud alpha monitoring policies create \
    --policy-from-file=log_alert_policy.json \
    --notification-channels=[YOUR_CHANNEL_ID]
```

> [!TIP]
> **Getting structured fields in Option 2:**
> Standard GCP Monitoring Webhooks transmit a standard GCP schema ( Incident ID, Scoping Project URL, State, Summary). If you want specific properties like `dag_id` in structured keys, consider using **Log-Based Metrics Extraction**.
