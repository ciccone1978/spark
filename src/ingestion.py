import os
import requests
from tqdm import tqdm
import wget

# Configurazione
DATASET_URL_BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet" 
OUTPUT_DIR = "../data/raw"
YEARS = [2024]
MONTHS = list(range(1, 13))

def ensure_dir(path):
    """Crea la cartella se non esiste"""
    if not os.path.exists(path):
        os.makedirs(path)

def check_file_exists(year, month):
    """Verifica se il file è già stato scaricato"""
    filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
    return os.path.isfile(os.path.join(OUTPUT_DIR, filename))

def download_file(url, output_path):
    """Scarica un file usando wget con progress bar"""
    try:
        wget.download(url, out=output_path, bar=None)
        print(f"\n✅ Scaricato: {url.split('/')[-1]}")
    except Exception as e:
        print(f"\n❌ Errore nel download di {url}: {e}")

def main():
    ensure_dir(OUTPUT_DIR)

    for year in YEARS:
        for month in tqdm(MONTHS, desc=f"Scaricando dati per {year}"):
            if check_file_exists(year, month):
                print(f"⏭️ Già presente: {year}-{month:02d}")
                continue

            url = DATASET_URL_BASE.format(year=year, month=month)
            output_path = os.path.join(OUTPUT_DIR, f"yellow_tripdata_{year}-{month:02d}.parquet")

            # Verifica se URL esiste prima di scaricare
            try:
                response = requests.head(url, timeout=10)
                if response.status_code == 404:
                    print(f"\n⚠️ File non trovato: {url}")
                    continue
            except requests.RequestException as e:
                print(f"\n⚠️ Impossibile verificare l'esistenza di {url}: {e}")
                continue

            download_file(url, output_path)

if __name__ == "__main__":
    main()