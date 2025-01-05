import hopsworks
import os
import pandas as pd
from hsfs.feature_group import FeatureGroup
from hsfs.feature_store import FeatureStore

from src.data_downloader import get_dataframes
from src.utils import load_config


def login(
    project="ID2223_Project",
) -> tuple[hopsworks.project.Project, FeatureStore]:
    project = hopsworks.login(
        api_key_value=os.environ["HOPSWORKS_API_KEY"],
        project=project,
    )
    fs = project.get_feature_store()

    return project, fs


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
        {
            "name": "league_over_percentage",
            "description": "Percentage of games that were over 2.5 goals",
        },
        {
            "name": "league_under_percentage",
            "description": "Percentage of games that were under 2.5 goals",
        },
        {"name": "ftour", "description": "Full time over/under result"},
    ]

    for desc in feature_descriptions:
        fg.update_feature_description(desc["name"].lower(), desc["description"])


def set_lag_feature_descriptions(fg: FeatureGroup):
    # Update feature descriptions
    feature_descriptions = [
        {
            "name": "index",
            "description": "Index of the match in the dataset from football-data.co.uk",
        },
        {"name": "datetime", "description": "Match datetime"},
        {"name": "HomeTeam", "description": "Home Team"},
        {"name": "AwayTeam", "description": "Away Team"},
        {"name": "hs_lags", "description": "Home shot lags"},
        {"name": "fthg_lags", "description": "Full time home goal lags"},
        {"name": "hthg_lags", "description": "Half time home goal lags"},
        {"name": "hst_lags", "description": "Home shot on target lags"},
        {"name": "as_lags", "description": "Away shot lags"},
        {"name": "ftag_lags", "description": "Full time away goal lags"},
        {"name": "htag_lags", "description": "Half time away goal lags"},
        {"name": "ast_lags", "description": "Away shots on target"},
    ]

    for desc in feature_descriptions:
        fg.update_feature_description(desc["name"].lower(), desc["description"])


def create_league_percentages(df: pd.DataFrame):
    # Calculate league percentage for Over/Under 2.5 goals
    df["total_goals"] = df["fthg"] + df["ftag"]
    df.insert(7, "ftour", df["total_goals"].apply(lambda x: "O" if x > 2.5 else "U"))

    df["cum_o"] = (df["ftour"] == "O").cumsum()
    df["cum_u"] = (df["ftour"] == "U").cumsum()

    # Calculate total games played up to each row (excluding the current row)
    df["total_games"] = df.index

    # Shift cumulative counts by 1 to exclude the current row's result
    df[["cum_o", "cum_u"]] = df[["cum_o", "cum_u"]].shift(1)

    # Fill NaNs with 0 for the first row
    df[["cum_o", "cum_u", "total_games"]] = df[
        ["cum_o", "cum_u", "total_games"]
    ].fillna(0)

    # Calculate global win/draw/loss percentages
    df["league_over_percentage"] = (df["cum_o"] / df["total_games"]).fillna(0)
    df["league_under_percentage"] = (df["cum_u"] / df["total_games"]).fillna(0)

    df.drop(columns=["total_goals", "cum_o", "cum_u", "total_games"], inplace=True)

    return df


def create_lag_df(df: pd.DataFrame, window_size):
    df_lags = df[["index", "datetime", "hometeam", "awayteam"]].copy()

    # Goal lag features
    # Create lag features using rolling window
    def get_lag_features(series, window_size):
        return pd.Series(
            series.shift().rolling(window=window_size, min_periods=1)
        ).apply(lambda x: list(x.dropna()))

    # Apply the function
    homeshot_lags = df.groupby("hometeam")["homeshots"].apply(
        lambda x: get_lag_features(x, window_size)
    )
    awayshot_lags = df.groupby("awayteam")["awayshots"].apply(
        lambda x: get_lag_features(x, window_size)
    )

    fulltime_home_goals_lags = df.groupby("hometeam")["fthg"].apply(
        lambda x: get_lag_features(x, window_size)
    )
    fulltime_away_goals_lags = df.groupby("awayteam")["ftag"].apply(
        lambda x: get_lag_features(x, window_size)
    )

    halftime_home_goals_lags = df.groupby("hometeam")["hthg"].apply(
        lambda x: get_lag_features(x, window_size)
    )
    halftime_away_goals_lags = df.groupby("awayteam")["htag"].apply(
        lambda x: get_lag_features(x, window_size)
    )

    homeshots_target_lags = df.groupby("hometeam")["hst"].apply(
        lambda x: get_lag_features(x, window_size)
    )
    awayshots_target_lags = df.groupby("awayteam")["ast"].apply(
        lambda x: get_lag_features(x, window_size)
    )

    home_lags = [
        ("hs_lags", homeshot_lags),
        ("fthg_lags", fulltime_home_goals_lags),
        ("hthg_lags", halftime_home_goals_lags),
        ("hst_lags", homeshots_target_lags),
    ]
    away_lags = [
        ("as_lags", awayshot_lags),
        ("ftag_lags", fulltime_away_goals_lags),
        ("htag_lags", halftime_away_goals_lags),
        ("ast_lags", awayshots_target_lags),
    ]

    for side, lags in [("hometeam", home_lags), ("awayteam", away_lags)]:
        for name, data in lags:
            for team in df_lags[side].unique():
                team_data = data[team]
                team_data.index = df_lags.loc[df_lags[side] == team].index
                df_lags.loc[df_lags[side] == team, name] = team_data

    return df_lags


def format_df(df: pd.DataFrame):
    df.columns = (
        df.columns.str.lower()
        .str.replace("<", "_lt_")
        .str.replace(">", "_gt_")
        .str.replace(".", "_")
    )  # Rename incompatible columns names for Hopsworks
    df.rename(
        columns={"date": "datetime", "as": "awayshots", "hs": "homeshots"},
        inplace=True,
    )  # Rename as to awayshot because as causes hopsworks to not upload data...
    df.reset_index(inplace=True)

    return df


def ingest(fs: FeatureStore, config: dict):
    feature_descriptions_set = True
    lags_feature_descriptions_set = True
    features = config["features"]
    league = config["league"]
    window_size = config["lag_window"]

    # Get the dataframe for the specified league in config
    dfs = get_dataframes(config)
    df = dfs[league]
    df = df[features].copy()

    # Format df
    df = format_df(df)

    # Calculate leage p
    df = create_league_percentages(df)

    # Create lags
    df_lags = create_lag_df(df, window_size)

    ## Insert data
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
        f"DATA: Inserting {len(df)} rows, dataset contains {len(dfs[league])} and featurestore already had {len(read_df)} rows"
    )
    fg.insert(df)

    if not feature_descriptions_set:
        set_feature_descriptions(fg)

    ## Insert lags
    # Get or create the 'football' feature group
    lags_fg = fs.get_or_create_feature_group(
        name=f"football_{league.lower()}_lags_{window_size}",
        version=1,
        description=f"Lags for historical football data for league {league} with window size {window_size}",
        primary_key=["index"],
        event_time="datetime",
        online_enabled=True,
    )

    # Try read data in the feature group in order to filter out data we already have
    try:
        lags_read_df = (
            lags_fg.select_all().read().sort_values("index", ignore_index=True)
        )
        df_lags = df_lags[~df_lags["index"].isin(lags_read_df["index"])].copy()
    except Exception:
        # If it fails it does not exist so set the descs
        lags_read_df = []
        lags_feature_descriptions_set = False

    print(
        f"LAGS: Inserting {len(df_lags)} rows, dataset contains {len(dfs[league])} and featurestore already had {len(lags_read_df)} rows"
    )
    lags_fg.insert(df_lags)

    if not lags_feature_descriptions_set:
        set_lag_feature_descriptions(lags_fg)


def run(config_path):
    config = load_config(config_path)
    project, fs = login()
    ingest(fs, config)
    hopsworks.logout()


if __name__ == "__main__":
    run()
