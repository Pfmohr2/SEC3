import requests
import os
import json
from utils.env_loader import load_env_file

DEPLOY_ENV = os.environ["DEPLOY_ENV"]

if DEPLOY_ENV == "dev":
    filename = "params.dev.env"
elif DEPLOY_ENV == "qa":
    filename = "params.qa.env"
elif DEPLOY_ENV == "prod":
    filename = "params.prod.env"
else:
    filename = "params.dev.env"


load_env_file(filename=filename, levels_up=2)
FLA_PROTOCOL_VERSION = os.environ["FLA_PROTOCOL_VERSION"]
TDP_API_URL = os.environ["TDP_API_URL"]
TDP_ORG = os.environ["TDP_ORG"]
AUTH_TOKEN = os.environ["AUTH_TOKEN"]
AGENT_ID = os.environ["AGENT_ID"]
OUTPUT_DIRECTORY = os.environ["OUTPUT_DIRECTORY"]


HEADERS = {
    "x-org-slug": TDP_ORG,
    "Content-Type": "application/json",
    "ts-auth-token": AUTH_TOKEN,
}

PIPELINE_NAME = "sec_push_to_fla_pipeline"
AGG_PIPELINE_NAME = "sec_aggregation_pipeline"
CATEGORY = "processed"
PROTOCOL_SLUG = "fla-file-drop"


def get_existing_pipeline(pipeline_name):
    url = f"{TDP_API_URL}/pipeline/search"
    res = requests.get(url, headers=HEADERS)  # or use HEADERS if token is JWT
    res.raise_for_status()
    pipelines = res.json()["hits"]
    return next((p for p in pipelines if p["name"] == pipeline_name), None)


def build_payload(agg_pipeline_id=None):
    return {
        "name": PIPELINE_NAME,
        "description": "Pipeline for SEC push to FLA",
        "triggerType": "custom",
        "triggerCondition": {
            "groupOperator": "AND",
            "groupLevel": 1,
            "groups": [
                {
                    "groupLevel": 2,
                    "groupOperator": "AND",
                    "groups": [
                        {
                            "key": "pipelineId",
                            "operator": "is",
                            "value": agg_pipeline_id,
                        }
                    ],
                },
                {
                    "groupLevel": 2,
                    "groupOperator": "AND",
                    "groups": [
                        {"key": "category", "operator": "is", "value": CATEGORY}
                    ],
                },
            ],
        },
        "protocolSlug": PROTOCOL_SLUG,  # matches your downloaded pipeline
        "protocolVersion": FLA_PROTOCOL_VERSION,  # matches your downloaded pipeline
        "pipelineConfig": {
            "agentId": AGENT_ID,
            "createPath": True,
            "outputDirectory": OUTPUT_DIRECTORY,
            "overwrite": True,
            "notificationsConfig": {
                "sendOnSuccessful": False,
                "sendOnFailed": True,
                "notificationEmailAddresses": ["dcp_lilly_tetra_dev@lists.lilly.com"],
            },
        },
        "masterScriptNamespace": "common",  # matches your downloaded pipeline
        "masterScriptSlug": PROTOCOL_SLUG,
        "masterScriptVersion": FLA_PROTOCOL_VERSION,
        "standby": 0,
        "retryBehavior": "exponential",
        "retryConfiguration": {"maxRetries": 3, "baseDelaySeconds": 60},
        "priority": 5,
        "maxParallelWorkflows": 2,
        "taskScriptTimeoutMins": None,
        "stepsConfig": {},  # empty object in your downloaded pipeline
        "protocolSchema": "v3",
        "artifactType": "protocol",
    }


def create_pipeline(agg_pipeline_id):
    payload = build_payload(agg_pipeline_id=agg_pipeline_id)
    print("Sending payload to create pipeline:")
    print(json.dumps(payload, indent=2))

    res = requests.post(f"{TDP_API_URL}/pipeline/create", headers=HEADERS, json=payload)

    try:
        res.raise_for_status()
        print(f"Created pipeline: {res.json()['id']}")
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error: {res.status_code}")
        print("Response:", res.text)
        raise


def update_pipeline(pipeline_id, agg_pipeline_id):
    payload = build_payload(agg_pipeline_id)
    print("Sending payload to update pipeline:")
    print(json.dumps(payload, indent=2))

    res = requests.post(
        f"{TDP_API_URL}/pipeline/update/{pipeline_id}",
        headers=HEADERS,
        json=payload,
    )

    try:
        res.raise_for_status()
        print(f"Updated pipeline: {pipeline_id}")
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error: {res.status_code}")
        print("Response:", res.text)
        raise


def normalize(obj):
    return json.dumps(obj, sort_keys=True)


def needs_update(existing: dict, desired: dict) -> bool:
    keys_to_compare = [
        "description",
        "triggerType",
        "triggerCondition",
        "protocolSlug",
        "protocolVersion",
        "pipelineConfig",
        "masterScriptNamespace",
        "masterScriptSlug",
        "masterScriptVersion",
        "standby",
        "retryBehavior",
    ]

    for key in keys_to_compare:
        existing_val = normalize(existing.get(key, None))
        desired_val = normalize(desired.get(key, None))
        if existing_val != desired_val:
            print(f"Change detected in field: '{key}'")
            print(f"Existing: {existing_val}")
            print(f"Desired : {desired_val}")
            return True
    return False


def main():
    pipeline = get_existing_pipeline(PIPELINE_NAME)
    agg_pipeline = get_existing_pipeline(AGG_PIPELINE_NAME)
    desired_payload = build_payload(agg_pipeline["id"])

    if pipeline:
        if needs_update(pipeline, desired_payload):
            update_pipeline(pipeline["id"], agg_pipeline["id"])
        else:
            print(f"No update needed for pipeline '{PIPELINE_NAME}'")
    else:
        create_pipeline(agg_pipeline["id"])


if __name__ == "__main__":
    main()