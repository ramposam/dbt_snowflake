import argparse
import os
import tempfile
from pathlib import Path
from core_utils import s3_utils
from core_utils.config_reader_dbt import ConfigReaderDBT
from datetime import datetime

from core_utils.dbt_models import DBTMirrorModel

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Generate dbt models from configs')

    parser.add_argument('--bucket_name',type=str, help='s3 bucket name', required=True)
    parser.add_argument('--configs_path',type=str,  help='s3 dataset configs path', required=True)
    parser.add_argument('--dataset_name',type=str,  help='s3 dataset folder name ', required=True)
    parser.add_argument('--mode',type=str, help='either local or airflow', required=True)
    parser.add_argument('--run_date', type=str, help='DAG Run Date(format=YYYY-MM-DD)', default=datetime.now().strftime("%Y-%m-%d"))
    parser.add_argument('--force_download', type=bool, help='True or False', default=False)
    args = parser.parse_args()


    if args.mode.lower() == "local":
        local_dir = os.path.join(os.getcwd(),"configs",args.dataset_name)
    else:
        temp_dir = tempfile.mkdtemp()
        local_dir = os.path.join(os.getcwd(),"configs",args.dataset_name)

    Path(local_dir).mkdir(exist_ok=True)

    # Example usage
    bucket_name = args.bucket_name # "rposam-devops-airflow"
    s3_folder = f"{os.path.join(args.configs_path,args.dataset_name)}" # "dataset_configs/dev"

    if args.force_download or not os.path.exists(local_dir):
        s3_utils.download_s3_folder(bucket_name, s3_folder, local_dir)
    else:
        print(f"Configs are already present at {local_dir}. So not downloading.")

    reader = ConfigReaderDBT(dataset_configs_path=local_dir,
                           dataset_name=args.dataset_name,
                           run_date=args.run_date)
    configs = reader.get_configs()
    print(f"Generating dbt models for the dataset: {args.dataset_name}")
    DBTMirrorModel(configs=configs).generate()

