from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import json
import os
import subprocess
from pathlib import Path
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from git import Repo
import shutil

# Default arguments for the DAG
default_args = {
    'owner': 'mlops_student',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'nasa_apod_etl_pipeline',
    default_args=default_args,
    description='NASA APOD ETL Pipeline with Airflow, DVC, and Postgres',
    schedule_interval=timedelta(days=1),  # Run daily
    start_date=days_ago(1),
    catchup=False,
    tags=['mlops', 'etl', 'nasa', 'apod'],
)


def extract_nasa_apod_data(**context):
    # NASA APOD API endpoint
    api_url = "https://api.nasa.gov/planetary/apod"
    api_key = "DEMO_KEY"
    
    # Parameters for the API request
    params = {
        'api_key': api_key,
        'hd': 'true'  # Request high-definition image URL if available
    }
    
    try:
        # Make API request
        print(f"Connecting to NASA APOD API: {api_url}")
        response = requests.get(api_url, params=params, timeout=30)
        response.raise_for_status()  # Raise an exception for bad status codes
        
        # Parse JSON response
        data = response.json()
        print(f"Successfully retrieved data: {json.dumps(data, indent=2)}")
        
        # Create data directory if it doesn't exist
        data_dir = Path('/opt/airflow/data')
        data_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate filename with timestamp
        execution_date = context.get('ds', datetime.now().strftime('%Y-%m-%d'))
        filename = f"nasa_apod_{execution_date}.json"
        filepath = data_dir / filename
        
        # Save raw data to JSON file
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"Data successfully saved to: {filepath}")
        
        # Push file path to XCom for downstream tasks
        return str(filepath)
        
    except requests.exceptions.RequestException as e:
        print(f"Error connecting to NASA APOD API: {str(e)}")
        raise
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON response: {str(e)}")
        raise
    except Exception as e:
        print(f"Unexpected error during extraction: {str(e)}")
        raise


def transform_nasa_apod_data(**context):
    # Pull file path from previous task via XCom
    ti = context['ti']
    json_filepath = ti.xcom_pull(task_ids='extract_nasa_apod_data')
    
    if not json_filepath:
        raise ValueError("No file path received from extract task")
    
    try:
        # Read JSON file
        print(f"Reading JSON file: {json_filepath}")
        with open(json_filepath, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)
        
        # Extract fields of interest
        transformed_data = {
            'date': raw_data.get('date', ''),
            'title': raw_data.get('title', ''),
            'explanation': raw_data.get('explanation', ''),
            'url': raw_data.get('url', ''),
            'hdurl': raw_data.get('hdurl', ''),
            'media_type': raw_data.get('media_type', ''),
            'service_version': raw_data.get('service_version', ''),
            'copyright': raw_data.get('copyright', '')
        }
        
        # Create Pandas DataFrame
        df = pd.DataFrame([transformed_data])
        
        # Ensure date is datetime type
        df['date'] = pd.to_datetime(df['date'])
        
        print(f"Transformed data:\n{df.to_string()}")
        print(f"DataFrame shape: {df.shape}")
        print(f"DataFrame columns: {df.columns.tolist()}")
        
        # Save to CSV
        data_dir = Path('/opt/airflow/data')
        csv_filepath = data_dir / 'apod_data.csv'
        df.to_csv(csv_filepath, index=False, encoding='utf-8')
        print(f"Transformed data saved to CSV: {csv_filepath}")
        
        # Return CSV file path for downstream tasks
        return str(csv_filepath)
        
    except FileNotFoundError as e:
        print(f"JSON file not found: {str(e)}")
        raise
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {str(e)}")
        raise
    except Exception as e:
        print(f"Unexpected error during transformation: {str(e)}")
        raise


def load_nasa_apod_data(**context):
    # Pull CSV file path from previous task via XCom
    ti = context['ti']
    csv_filepath = ti.xcom_pull(task_ids='transform_nasa_apod_data')

    if not csv_filepath:
        raise ValueError("No CSV file path received from transform task")

    try:
        # Read CSV file
        print(f"Reading CSV file: {csv_filepath}")
        df = pd.read_csv(csv_filepath)

        # PostgreSQL connection parameters
        conn_params = {
            'host': 'postgres',
            'database': 'airflow',
            'user': 'airflow',
            'password': 'airflow',
            'port': 5432
        }

        # Connect to PostgreSQL
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()

        # Ensure table exists (create if not exists)
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS nasa_apod_data (
            id SERIAL PRIMARY KEY,
            date DATE NOT NULL UNIQUE,
            title VARCHAR(500) NOT NULL,
            explanation TEXT,
            url VARCHAR(1000),
            hdurl VARCHAR(1000),
            media_type VARCHAR(50),
            service_version VARCHAR(50),
            copyright VARCHAR(500),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cursor.execute(create_table_sql)
        conn.commit()
        print("PostgreSQL table 'nasa_apod_data' ensured to exist")

        # Prepare data for insertion (upsert - update if exists, insert if not)
        upsert_sql = """
        INSERT INTO nasa_apod_data (date, title, explanation, url, hdurl, media_type, service_version, copyright)
        VALUES %s
        ON CONFLICT (date)
        DO UPDATE SET
            title = EXCLUDED.title,
            explanation = EXCLUDED.explanation,
            url = EXCLUDED.url,
            hdurl = EXCLUDED.hdurl,
            media_type = EXCLUDED.media_type,
            service_version = EXCLUDED.service_version,
            copyright = EXCLUDED.copyright,
            updated_at = CURRENT_TIMESTAMP;
        """

        # Convert DataFrame to list of tuples
        values = [
            (
                row['date'],
                row['title'],
                row['explanation'] if pd.notna(row['explanation']) else None,
                row['url'] if pd.notna(row['url']) else None,
                row['hdurl'] if pd.notna(row['hdurl']) else None,
                row['media_type'] if pd.notna(row['media_type']) else None,
                row['service_version'] if pd.notna(row['service_version']) else None,
                row['copyright'] if pd.notna(row['copyright']) else None
            )
            for _, row in df.iterrows()
        ]

        # Execute upsert
        execute_values(cursor, upsert_sql, values)
        conn.commit()

        print(f"Successfully loaded {len(values)} record(s) into PostgreSQL")

        # Verify insertion
        cursor.execute("SELECT COUNT(*) FROM nasa_apod_data")
        count = cursor.fetchone()[0]
        print(f"Total records in database: {count}")

        cursor.close()
        conn.close()

        # Ensure CSV file exists locally
        csv_path = Path(csv_filepath)
        if not csv_path.exists():
            df.to_csv(csv_path, index=False, encoding='utf-8')
            print(f"CSV file ensured at: {csv_path}")

        return str(csv_filepath)

    except psycopg2.Error as e:
        print(f"PostgreSQL error: {str(e)}")
        raise
    except Exception as e:
        print(f"Unexpected error during loading: {str(e)}")
        raise


def version_data_with_dvc(**context):
    """
    Copies CSV to project directory, tracks it with DVC, and returns the .dvc metadata path.
    """
    ti = context['ti']
    csv_filepath = ti.xcom_pull(task_ids='load_nasa_apod_data')
    
    if not csv_filepath:
        raise ValueError("No CSV file path received from load task")
    
    csv_path = Path(csv_filepath)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_filepath}")

    project_root = Path('/opt/airflow/project')
    project_root.mkdir(parents=True, exist_ok=True)
    project_data_dir = project_root / 'data'
    project_data_dir.mkdir(parents=True, exist_ok=True)

    # Copy CSV to project directory for DVC tracking
    project_csv_path = project_data_dir / csv_path.name
    shutil.copy2(csv_path, project_csv_path)
    print(f"Copied CSV to project directory: {project_csv_path}")

    # Initialize DVC if not already
    dvc_dir = project_root / '.dvc'
    if not dvc_dir.exists():
        print("Initializing DVC repository...")
        subprocess.run(['dvc', 'init', '--no-scm'], cwd=str(project_root), check=False)

    # Add CSV to DVC
    csv_relative_path = project_csv_path.relative_to(project_root)
    print(f"Adding {csv_relative_path} to DVC...")
    subprocess.run(['dvc', 'add', str(csv_relative_path)], cwd=str(project_root), check=True)

    # Return the .dvc metadata file path
    dvc_metadata_file = project_root / f"{csv_relative_path}.dvc"
    if not dvc_metadata_file.exists():
        raise FileNotFoundError("DVC metadata file not found after adding file to DVC")
    
    print(f"DVC metadata file created: {dvc_metadata_file}")
    return str(dvc_metadata_file)


def commit_dvc_metadata_to_git(**context):
    """
    Commits DVC metadata to a Git repository and pushes to a remote (e.g., GitHub).
    """
    ti = context['ti']
    dvc_metadata_filepath = ti.xcom_pull(task_ids='version_data_with_dvc')

    if not dvc_metadata_filepath:
        print("No DVC metadata file path received. Skipping Git commit.")
        return None

    dvc_metadata_path = Path(dvc_metadata_filepath)
    project_root = Path('/opt/airflow/project')

    # Initialize Git repo if not exists
    if not (project_root / '.git').exists():
        print("Initializing Git repository...")
        repo = Repo.init(str(project_root))
    else:
        repo = Repo(str(project_root))

    # Configure Git user
    repo.config_writer().set_value("user", "name", "MLOps Pipeline").release()
    repo.config_writer().set_value("user", "email", "mlops@airflow.local").release()

    # Stage DVC file and .gitignore/.dvcignore if they exist
    files_to_add = [str(dvc_metadata_path.relative_to(project_root))]
    for f in ['.dvcignore', '.gitignore']:
        path = project_root / f
        if path.exists():
            files_to_add.append(str(path.relative_to(project_root)))
    repo.index.add(files_to_add)

    # Commit changes
    if repo.is_dirty() or repo.untracked_files:
        execution_date = context.get('ds', datetime.now().strftime('%Y-%m-%d'))
        commit_message = f"Add DVC metadata for NASA APOD data - {execution_date}"
        commit = repo.index.commit(commit_message)
        print(f"Committed to Git: {commit.hexsha} - {commit_message}")

        # Push to remote if remote named 'origin' exists
        if 'origin' in repo.remotes:
            print("Pushing commit to remote 'origin'...")
            repo.remotes.origin.push()
            print("Push successful")
        else:
            print("No remote named 'origin' configured. Skipping push.")
        
        return commit.hexsha
    else:
        print("No changes to commit. Skipping Git commit.")
        return repo.head.commit.hexsha if repo.head.is_valid() else None




# Step 1: Extract Task
extract_task = PythonOperator(
    task_id='extract_nasa_apod_data',
    python_callable=extract_nasa_apod_data,
    dag=dag,
)

# Step 2: Transform Task
transform_task = PythonOperator(
    task_id='transform_nasa_apod_data',
    python_callable=transform_nasa_apod_data,
    dag=dag,
)

# Step 3: Load Task
load_task = PythonOperator(
    task_id='load_nasa_apod_data',
    python_callable=load_nasa_apod_data,
    dag=dag,
)

# Step 4: DVC Versioning Task
dvc_version_task = PythonOperator(
    task_id='version_data_with_dvc',
    python_callable=version_data_with_dvc,
    dag=dag,
)

# Step 5: Git Commit Task
git_commit_task = PythonOperator(
    task_id='commit_dvc_metadata_to_git',
    python_callable=commit_dvc_metadata_to_git,
    dag=dag,
)

# Set task dependencies - sequential pipeline
extract_task >> transform_task >> load_task >> dvc_version_task >> git_commit_task

