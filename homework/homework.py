import os
import pandas as pd
import zipfile

def clean_campaign_data():
    """Procesa los archivos CSV comprimidos en ZIP y genera los archivos limpios en 'files/output/'."""

    input_dir = "files/input/"
    output_dir = "files/output/"
    os.makedirs(output_dir, exist_ok=True)

    data_frames = []

    # Leer todos los archivos ZIP en la carpeta de entrada
    for file in os.listdir(input_dir):
        if file.endswith(".zip"):
            with zipfile.ZipFile(os.path.join(input_dir, file), "r") as z:
                for csv_file in z.namelist():
                    with z.open(csv_file) as f:
                        df = pd.read_csv(f, encoding="utf-8")
                        data_frames.append(df)

    # Unir todos los DataFrames en uno solo
    df = pd.concat(data_frames, ignore_index=True)

    # Procesamiento de 'client.csv'
    client_df = df[["client_id", "age", "job", "marital", "education", "credit_default", "mortgage"]].copy()
    client_df["job"] = client_df["job"].str.replace(".", "", regex=False).str.replace("-", "_", regex=False)
    client_df["education"] = client_df["education"].str.replace(".", "_", regex=False).replace("unknown", pd.NA)
    client_df["credit_default"] = client_df["credit_default"].apply(lambda x: 1 if x == "yes" else 0)
    client_df["mortgage"] = client_df["mortgage"].apply(lambda x: 1 if x == "yes" else 0)
    client_df.to_csv(os.path.join(output_dir, "client.csv"), index=False)

    # Procesamiento de 'campaign.csv'
    campaign_df = df[[
        "client_id", "number_contacts", "contact_duration",
        "previous_campaign_contacts", "previous_outcome", "campaign_outcome", "day", "month"
    ]].copy()
    campaign_df["previous_outcome"] = campaign_df["previous_outcome"].apply(lambda x: 1 if x == "success" else 0)
    campaign_df["campaign_outcome"] = campaign_df["campaign_outcome"].apply(lambda x: 1 if x == "yes" else 0)
    
    # Convertir "day" y "month" en una fecha con año 2022
    month_map = {
        "jan": "01", "feb": "02", "mar": "03", "apr": "04", "may": "05", "jun": "06",
        "jul": "07", "aug": "08", "sep": "09", "oct": "10", "nov": "11", "dec": "12"
    }
    campaign_df["month"] = campaign_df["month"].str.lower().map(month_map)
    campaign_df["last_contact_date"] = "2022-" + campaign_df["month"] + "-" + campaign_df["day"].astype(str).str.zfill(2)
    campaign_df = campaign_df.drop(columns=["day", "month"])  # Eliminar columnas originales
    campaign_df.to_csv(os.path.join(output_dir, "campaign.csv"), index=False)

    # Procesamiento de 'economics.csv'
    economics_df = df[["client_id", "cons_price_idx", "euribor_three_months"]].copy()
    economics_df.to_csv(os.path.join(output_dir, "economics.csv"), index=False)

if __name__ == "__main__":
    clean_campaign_data()
