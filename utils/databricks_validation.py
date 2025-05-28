import requests
import argparse
import os

def list_jobs_by_tag(instance_url, token, tag_key, tag_value):
    url = f"{instance_url}/api/2.1/jobs/list"
    headers = {
        "Authorization": f"Bearer {token}"
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    jobs = response.json().get("jobs", [])
    matched_jobs = []

    for job in jobs:
        tags = job.get("settings", {}).get("tags", {})
        if tags.get(tag_key) == tag_value:
            matched_jobs.append({
                "job_id": job.get("job_id"),
                "name": job.get("settings", {}).get("name"),
                "tags": tags
            })

    return matched_jobs

def main():
    parser = argparse.ArgumentParser(description="List Databricks jobs by tag")
    parser.add_argument("--instance", required=True, help="Databricks instance URL (e.g. https://adb-123456.0.azuredatabricks.net)")
    parser.add_argument("--token", required=True, help="Databricks personal access token")
    parser.add_argument("--tag-key", required=True, help="Tag key to filter by")
    parser.add_argument("--tag-value", required=True, help="Tag value to filter by")

    args = parser.parse_args()

    jobs = list_jobs_by_tag(args.instance, args.token, args.tag_key, args.tag_value)
    
    if not jobs:
        print(f"No jobs found with tag {args.tag_key}={args.tag_value}")
    else:
        print(f"Found {len(jobs)} job(s):\n")
        for job in jobs:
            print(f"- Job ID: {job['job_id']}, Name: {job['name']}, Tags: {job['tags']}")

if __name__ == "__main__":
    main()


