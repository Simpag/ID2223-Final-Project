import hopsworks
import yaml
from hsfs.feature_group import FeatureGroup
from hsfs.feature_store import FeatureStore

from src.data_downloader import get_dataframes


def login(
    api_key_file="secret.txt",
    project="ID2223_Project",
) -> tuple[hopsworks.project.Project, FeatureStore]:
    project = hopsworks.login(api_key_file=api_key_file, project=project)
    fs = project.get_feature_store()

    return project, fs


def load_config() -> dict:
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)

    return config


def set_feature_descriptions(fg: FeatureGroup):
    # Update feature descriptions
    feature_descriptions = [
        {
            "name": "index",
            "description": "Index of the match in the dataset from football-data.co.uk",
        },
        {"name": "datetime", "description": "Match datetime"},
        {"name": "HomeTeam", "description": "Home Team"},
        {"name": "AwayTeam", "description": "Away Team"},
        {"name": "FTHG", "description": "Full Time Home Team Goals"},
        {"name": "FTAG", "description": "Full Time Away Team Goals"},
        {
            "name": "FTR",
            "description": "Full Time Result (H=Home Win, D=Draw, A=Away Win)",
        },
        {"name": "HTHG", "description": "Half Time Home Team Goals"},
        {"name": "HTAG", "description": "Half Time Away Team Goals"},
        {
            "name": "HTR",
            "description": "Half Time Result (H=Home Win, D=Draw, A=Away Win)",
        },
        {"name": "homeshots", "description": "Home Team Shots"},
        {"name": "awayshots", "description": "Away Team Shots"},
        {"name": "HST", "description": "Home Team Shots on Target"},
        {"name": "AST", "description": "Away Team Shots on Target"},
        {"name": "AvgH", "description": "Market average home win odds"},
        {"name": "AvgD", "description": "Market average draw win odds"},
        {"name": "AvgA", "description": "Market average away win odds"},
        {"name": "Avg_gt_2_5", "description": "Market average over 2.5 goals"},
        {"name": "Avg_lt_2_5", "description": "Market average under 2.5 goals"},
    ]

    for desc in feature_descriptions:
        fg.update_feature_description(desc["name"].lower(), desc["description"])


def ingest(fs: FeatureStore, config: dict):
    feature_descriptions_set = True
    features = config["features"]
    league = config["league"]
    dfs = get_dataframes()
    df = dfs[league]
    df = df[features].copy()
    df.columns = (
        df.columns.str.lower()
        .str.replace("<", "_lt_")
        .str.replace(">", "_gt_")
        .str.replace(".", "_")
    )  # Rename incompatible columns names for Hopsworks
    df.rename(
        columns={"date": "datetime", "as": "awayshots", "hs": "homeshots"}, inplace=True
    )  # Rename as to awayshot because as causes hopsworks to not upload data...
    df.reset_index(inplace=True)

    # Get or create the 'football' feature group
    fg = fs.get_or_create_feature_group(
        name=f"football_{league.lower()}",
        version=1,
        description=f"Historical football data for league {league}",
        primary_key=["index"],
        event_time="datetime",
        online_enabled=True,
    )

    # Try read data in the feature group in order to filter out data we already have
    try:
        read_df = fg.select_all().read().sort_values("index", ignore_index=True)
        df = df[~df["index"].isin(read_df["index"])].copy()
    except Exception:
        # If it fails it does not exist so set the descs
        read_df = []
        feature_descriptions_set = False

    print(
        f"Inserting {len(df)} rows, dataset contains {len(dfs[league])} and featurestore already had {len(read_df)} rows"
    )
    fg.insert(df)

    if not feature_descriptions_set:
        set_feature_descriptions(fg)


def run():
    config = load_config()
    project, fs = login()
    ingest(fs, config)


if __name__ == "__main__":
    run()
