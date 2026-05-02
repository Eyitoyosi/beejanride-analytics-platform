import subprocess
import os

DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR")
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR")


def run_dbt_command(command: str):
    full_cmd = f"dbt {command} --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"
    
    result = subprocess.run(full_cmd, shell=True, capture_output=True, text=True)

    print(result.stdout)

    if result.returncode != 0:
        raise Exception(f"dbt command failed: {result.stderr}")
    

    