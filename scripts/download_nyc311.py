from kaggle.api.kaggle_api_extended import KaggleApi
import os
import zipfile


DATASET_SLUG = "nidhirastogi/311-service-requests-from-2010-to-present"
# FILE_NAME = "311_Service_Requests_from_2011.csv"      file with 1,2 GB
FILE_NAME = "311_Service_Requests_from_2010_to_Present.csv/311_Service_Requests_from_2010_to_Present.csv"    #file with 11 GB




def download_nyc311_from_kaggle(
    dataset: str = DATASET_SLUG,
    file_name: str = FILE_NAME,
    dest: str = "./data"
) -> str:
    os.makedirs(dest, exist_ok=True)

    api = KaggleApi()
    api.authenticate()

    print(f"[⬇] Downloading {file_name} from Kaggle dataset {dataset} ...")
    api.dataset_download_file(dataset, file_name, path=dest, force=True)
    
    base_filename = os.path.basename(file_name)
    print(f"[🔍] Base filename: {base_filename}")

    zip_path = os.path.join(dest, base_filename + ".zip")
    csv_path = os.path.join(dest, base_filename)

    if os.path.exists(zip_path):
        print(f"[📦] Extracting {zip_path}...")
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(dest)
        os.remove(zip_path)
        print(f"[✅] Extracted to {csv_path}")
    else:
        print(f"[⚠️] No ZIP found at {zip_path} (may already be extracted)")

    print(f"[✅] Kaggle file ready at {csv_path}")
    return csv_path


if __name__ == "__main__":
    download_nyc311_from_kaggle()
