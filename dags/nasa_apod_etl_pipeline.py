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
    Version the CSV file using DVC inside Docker safely.
    Logs errors instead of failing the DAG.
    """
    ti = context['ti']
    csv_filepath = ti.xcom_pull(task_ids='load_nasa_apod_data')
    
    if not csv_filepath:
        print("Warning: No CSV file path received from load task. Skipping DVC versioning.")
        return None
    
    try:
        csv_path = Path(csv_filepath)
        if not csv_path.exists():
            print(f"Warning: CSV file not found: {csv_filepath}. Skipping DVC versioning.")
            return None
        
        # Project root (mounted volume)
        project_root = Path('/opt/airflow/project')
        project_root.mkdir(parents=True, exist_ok=True)
        
        # Initialize DVC if not already initialized
        dvc_dir = project_root / '.dvc'
        if not dvc_dir.exists():
            print("Initializing DVC repository...")
            result = subprocess.run(
                ['dvc', 'init', '--no-scm'],
                cwd=str(project_root),
                capture_output=True,
                text=True
            )
            if result.returncode != 0:
                if "already initialized" in result.stderr.lower() or "already initialized" in result.stdout.lower():
                    print("DVC repo already initialized.")
                else:
                    print(f"Warning: DVC init failed: {result.stderr}")
        
        # Copy CSV to project data directory for DVC
        project_data_dir = project_root / 'data'
        project_data_dir.mkdir(parents=True, exist_ok=True)
        project_csv_path = project_data_dir / csv_path.name
        shutil.copy2(csv_path, project_csv_path)
        print(f"Copied CSV to project directory: {project_csv_path}")
        
        # Add CSV to DVC
        csv_relative_path = project_csv_path.relative_to(project_root)
        print(f"Adding {csv_relative_path} to DVC...")
        
        result = subprocess.run(
            ['dvc', 'add', str(csv_relative_path)],
            cwd=str(project_root),
            capture_output=True,
            text=True
        )
        print(f"DVC add output: {result.stdout}")
        print(f"DVC add error: {result.stderr}")
        
        if result.returncode != 0 and "already added" not in result.stderr.lower():
            print(f"Warning: DVC add may have failed for {csv_relative_path}")
        
        dvc_metadata_file = project_root / f"{csv_relative_path}.dvc"
        if dvc_metadata_file.exists():
            print(f"DVC metadata file created: {dvc_metadata_file}")
            return str(dvc_metadata_file)
        else:
            print("Warning: DVC metadata file not found after adding file to DVC")
            return None
        
    except Exception as e:
        print(f"Warning: Unexpected error during DVC versioning: {e}")
        return None


def commit_dvc_metadata_to_git(**context):
    from git import Repo, GitCommandError
    import os

    # Pull DVC metadata file path from XCom
    ti = context['ti']
    dvc_metadata_filepath = ti.xcom_pull(task_ids='version_data_with_dvc')

    if not dvc_metadata_filepath:
        raise ValueError("No DVC metadata file path received from versioning task")

    try:
        project_root = Path('/opt/airflow/project')
        dvc_metadata_path = Path(dvc_metadata_filepath)

        if not dvc_metadata_path.exists():
            raise FileNotFoundError(f"DVC metadata file not found: {dvc_metadata_filepath}")

        # Initialize Git repo if it doesn't exist
        git_dir = project_root / '.git'
        if not git_dir.exists():
            print("Initializing Git repository...")
            repo = Repo.init(str(project_root))
        else:
            repo = Repo(str(project_root))

        # Configure Git user
        try:
            repo.config_writer().set_value("user", "name", "MuhammadZunair19").release()
            repo.config_writer().set_value("user", "email", "lifedbs20@gmail.com").release()
        except:
            pass

        # Stage DVC metadata
        repo.index.add([str(dvc_metadata_path.relative_to(project_root))])

        # Add .dvcignore and .gitignore if exist
        for f in ['.dvcignore', '.gitignore']:
            path = project_root / f
            if path.exists():
                repo.index.add([str(path.relative_to(project_root))])

        # Commit changes if there are any
        if repo.is_dirty() or repo.untracked_files:
            execution_date = context.get('ds', datetime.now().strftime('%Y-%m-%d'))
            commit_message = f"Add DVC metadata for NASA APOD data - {execution_date}"
            commit = repo.index.commit(commit_message)
            print(f"Committed locally: {commit_message}")

        # Set remote GitHub repo (replace USERNAME/REPO with yours)
        github_user = os.environ.get("MuhammadZunair19")  # Or Airflow Variable
        github_pat = os.environ.get("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")        # Or Airflow Variable
        if not github_user or not github_pat:
            print("GitHub credentials not set. Skipping push to remote.")
            return None

        github_repo_url = f"https://{github_user}:{github_pat}@github.com/{github_user}/MLOps-Assignment-3.git"

        # Add remote if not exist
        if 'origin' not in [r.name for r in repo.remotes]:
            repo.create_remote('origin', github_repo_url)
        else:
            repo.remotes.origin.set_url(github_repo_url)

        # Push to main branch
        print("Pushing commits to GitHub...")
        repo.remotes.origin.push('main')
        print("Push to GitHub successful.")

        return repo.head.commit.hexsha

    except GitCommandError as e:
        print(f"Git command failed: {str(e)}")
        raise
    except Exception as e:
        print(f"Unexpected error during Git commit/push: {str(e)}")
        raise




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

