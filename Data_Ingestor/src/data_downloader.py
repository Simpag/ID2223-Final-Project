import pandas as pd
import os
import requests
import yaml

from bs4 import BeautifulSoup
from datetime import datetime
from io import BytesIO


def load_config():
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)

    return config


def setup_dirs(config: dict):
    # Directory to save the downloaded file
    os.makedirs(config["save_dir"], exist_ok=True)


def download_data(config: dict) -> bytes:
    # Send a GET request to fetch the page content
    response = requests.get(config["url"])
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, "html.parser")

        # Find the <a> tag containing the text "Excel"
        excel_link = soup.find("a", string=config["file_name"])

        if excel_link and "href" in excel_link.attrs:
            # Get the href attribute (URL to the Excel file)
            excel_url = excel_link["href"]

            # Ensure the URL is absolute
            if not excel_url.startswith(("http", "https")):
                excel_url = f"https://www.football-data.co.uk/{excel_url}"

            # Download the Excel file
            file_response = requests.get(excel_url)
            if file_response.status_code == 200:
                return file_response.content
            else:
                print("Failed to download the Excel file.")
        else:
            print("Could not find the 'Excel' link on the page.")
    else:
        print(f"Failed to fetch the webpage. Status code: {response.status_code}")


def extract_data(
    config: dict, data: bytes, save_to_file=False
) -> dict[str, pd.DataFrame]:
    dfs = dict()
    columns_with_na = list()

    if save_to_file:
        setup_dirs(config)

    # Read the Excel file and load specified sheets
    with pd.ExcelFile(BytesIO(data)) as excel_file:
        for sheet_name in config["sheets_mapping"]:
            if sheet_name in excel_file.sheet_names:
                df: pd.DataFrame = excel_file.parse(sheet_name)

                # Fill missing Odds values
                for target_col, source_col in config["fill_columns"].items():
                    if target_col in df.columns and source_col in df.columns:
                        df[target_col] = df[target_col].fillna(df[source_col])
                        # print(
                        #    f"Filled missing values in '{target_col}' with values from '{source_col}'."
                        # )

                # Convert to date
                if "Date" in df.columns:
                    df["Date"] = pd.to_datetime(
                        df["Date"].dt.date.combine(
                            df["Time"], lambda x, y: datetime.combine(x, y)
                        ),
                        errors="coerce",
                    )
                    df.drop(columns=["Time"], inplace=True)

                # Save or merge
                if save_to_file:
                    df = save_or_concat_data(config, df, sheet_name)

                dfs[sheet_name] = df

                # Check if there are missing values in interesting cols
                cols = config["features"]
                with_na = df[cols].columns[df[cols].isna().any()].tolist()
                if len(with_na) > 0:
                    columns_with_na.append(
                        (with_na, config["sheets_mapping"][sheet_name])
                    )
            else:
                print(f"Sheet '{sheet_name}' not found in the Excel file.")

    for cwn, sheet in columns_with_na:
        print(f"Columns with NaN values: {cwn} in {sheet}")

    return dfs


def save_or_concat_data(config: dict, df: pd.DataFrame, sheet_name: str):
    # Save the concatenated DataFrame to a new Excel file
    concat_file_path = os.path.join(
        config["save_dir"],
        f"{config['sheets_mapping'][sheet_name]}.xlsx",
    )

    num_rows_merged = None
    file_exists = os.path.exists(concat_file_path)
    if file_exists:
        main_df = pd.read_excel(concat_file_path)
        main_df["Date"] = pd.to_datetime(main_df["Date"])

        df = (
            pd.concat([main_df, df], ignore_index=True)
            .drop_duplicates(subset=["Date", "HomeTeam", "AwayTeam"], keep="first")
            .sort_values("Date", ignore_index=True)
        )
        num_rows_merged = len(df) - len(main_df)

    df.to_excel(concat_file_path, index=False)
    print(
        f"Sheet {sheet_name} {f'merged {num_rows_merged} rows' if file_exists else 'saved'} to: {concat_file_path}"
    )

    return df


def get_dataframes() -> dict[str, pd.DataFrame]:
    config = load_config()
    data = download_data(config)
    dfs = extract_data(config, data, save_to_file=False)

    return dfs


if __name__ == "__main__":
    config = load_config()
    data = download_data(config)
    extract_data(config, data, save_to_file=True)
