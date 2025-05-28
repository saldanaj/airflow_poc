# airflow_poc

This repository contains a proof-of-concept (POC) setup for Apache Airflow, including sample DAGs, Databricks integration, and utility scripts for local development and testing.

## Repository Structure

- `dags/`  
  Contains Airflow DAGs, including:
  - `helloworld.py`: Example "Hello World" DAG.
  - `databricks_workflow.py`: DAG to trigger a Databricks job.
  - `requirements.txt`: Python dependencies for DAGs.

- `samples/`  
  Example DAGs and workflows, e.g.:
  - `raw_to_cleansed_dag.py`: Sample pipeline DAG with dbt and DataDog integration.

- `utils/`  
  Utility scripts, e.g.:
  - `read_db.py`: Script to inspect Airflow's SQLite database connections.

- Shell scripts for local setup and Airflow service management:
  - `local_setup.sh`: Initializes Airflow, resets DB, creates admin user, and sets up a Databricks connection.
  - `airflow_db_init.sh`, `airflow_webserver_init.sh`, `airflow_scheduler_init.sh`: Scripts to manage Airflow services.

- `requirements.txt`: Main Python dependencies for the project.

## Local Setup

1. **Clone the repository**
    ```sh
    git clone <repo-url>
    cd airflow_poc
    ```

2. **Install Python dependencies**
    ```sh
    python3 -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt
    pip install -r dags/requirements.txt
    ```

3. **Initialize Airflow and set up the environment**
    ```sh
    bash local_setup.sh
    ```

    This will:
    - Set up Airflow home
    - Reset and initialize the Airflow metadata database
    - Create an admin user (`admin`/`admin`)
    - Add a default Databricks connection

4. **Start Airflow services**

    In separate terminals, run:
    ```sh
    bash airflow_webserver_init.sh
    ```
    and
    ```sh
    bash airflow_scheduler_init.sh
    ```

5. **Access the Airflow UI**

    Open [http://localhost:8080](http://localhost:8080) in your browser and log in with the admin credentials.

## Notes

- The Airflow database uses SQLite by default and is ignored by git.
- Example DAGs are provided for both simple and Databricks-integrated workflows.
- Utility scripts are available for inspecting the Airflow DB and managing connections.

---
For more details, see the comments in each script and DAG file.
