import papermill as pm
import os
from src.utils.path_utils import find_project_root

def run_pipeline():
    BASE_DIR=find_project_root()
    config_path = BASE_DIR / 'configurations' / 'config.json'
    output_dir = BASE_DIR / "notebooks" / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    params = {
        "config_path": str(config_path),
        "base_dir":str(BASE_DIR)
    }

    notebooks = [
        ("notebooks/ingest_bronze.ipynb", output_dir / "ingest_bronze_output.ipynb"),
        ("notebooks/transform_silver.ipynb", output_dir / "transform_silver_output.ipynb"),
        ("notebooks/enrich_gold.ipynb", output_dir / "enrich_gold_output.ipynb"),
    ]

    for input_nb, output_nb in notebooks:
        print(f"Running {input_nb} ...")
        pm.execute_notebook(
            str(BASE_DIR / input_nb),
            str(output_nb),
            parameters=params
        )
    print(f"Finished {input_nb}\n")

if __name__ == "__main__":
    run_pipeline()