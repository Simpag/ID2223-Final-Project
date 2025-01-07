import joblib
import os
import pandas as pd
import time
import xgboost as xgb

from datetime import datetime, timedelta
from hsml.model_registry import ModelRegistry

from src.daily_odds import get_games_today
from src.utils import login, logout


class Predictor:
    def __init__(
        self,
        league,
        window_size,
    ):
        self.league = league
        self.window_size = window_size

    def predict_and_save(self):
        self._login()

        attemps = 0
        data = None
        while attemps < 3:
            try:
                data = self._get_data()
                break
            except Exception:
                attemps += 1
                time.sleep(0.25)

        if data is None:
            print("No matches today!")
            return

        model = self._load_model()

        # Use models feature names in to align the input data with expected column order
        data["predictions"] = model.predict(data[model.feature_names_in_])

        self._save_predictions(data)

        logout()

    def _save_predictions(self, data: pd.DataFrame):
        fg = self.fs.get_or_create_feature_group(
            name=f"football_{self.league.lower()}_predictions",
            version=1,
            description=f"Predictions for league {self.league}",
            primary_key=["datetime", "hometeam", "awayteam"],
            event_time="datetime",
            online_enabled=False,
        )

        print(f"Inserting {len(data)} rows... \n")
        fg.insert(data)

    def _get_data(self) -> None | pd.DataFrame:
        data = pd.DataFrame()
        games = get_games_today()

        if len(games) <= 0:
            return None

        main_fg, lags_fg = self._get_football_fgs()

        # Get league percentages
        # Query the latest row from main_fg based
        main_fg_query = main_fg.select(
            [
                "datetime",
                "league_over_percentage",
                "league_under_percentage",
            ]
        ).filter(
            main_fg.datetime
            >= (datetime.today() - timedelta(weeks=1)).strftime("%Y-%m-%d")
        )
        main_df = main_fg_query.read()
        main_df = main_df[main_df["datetime"] == main_df["datetime"].max()].reset_index(
            drop=True
        )

        # Query lags_fg for the row where "hometeam" or "awayteam" matches
        home_teams, away_teams = [g["home"] for g in games], [g["away"] for g in games]
        lags_home_query = lags_fg.select_all().filter(
            lags_fg.hometeam.isin(home_teams) | lags_fg.awayteam.isin(away_teams)
        )
        lags_df = lags_home_query.read()

        for game in games:
            home_lags = lags_df[lags_df["hometeam"] == game["home"]]
            home_lags = home_lags[home_lags["datetime"] == home_lags["datetime"].max()]

            away_lags = lags_df[lags_df["awayteam"] == game["away"]]
            away_lags = away_lags[away_lags["datetime"] == away_lags["datetime"].max()]
            df = pd.DataFrame()
            df["league_over_percentage"] = main_df["league_over_percentage"]
            df["league_under_percentage"] = main_df["league_under_percentage"]
            df["datetime"] = pd.to_datetime(game["date"])
            df["hometeam"] = game["home"]
            df["awayteam"] = game["away"]
            df["avgh"] = float(game["home_odds"])
            df["avgd"] = float(game["draw_odds"])
            df["avga"] = float(game["away_odds"])
            df["avg_gt_2_5"] = float(game["over25"])
            df["avg_lt_2_5"] = float(game["under25"])
            df = pd.concat(
                [df, self._get_sided_lags(home_lags, home=True).reset_index(drop=True)],
                axis=1,
            )
            df = pd.concat(
                [
                    df,
                    self._get_sided_lags(away_lags, home=False).reset_index(drop=True),
                ],
                axis=1,
            )

            data = pd.concat([data, df])

        return data

    def _get_sided_lags(self, df: pd.DataFrame, home: bool):
        # Identify columns with lists
        if home:
            lag_columns = ["hs_lags", "fthg_lags", "hthg_lags", "hst_lags"]
        else:
            lag_columns = ["as_lags", "ftag_lags", "htag_lags", "ast_lags"]

        # Get actual lag columns (*_lags_<window_size>)
        lag_columns = [
            col
            for col in df.columns
            if any(col.startswith(prefix) for prefix in lag_columns)
        ]

        return df[lag_columns]

    def _login(self):
        # connect with Hopsworks
        self.project, self.fs = login()

        # get Hopsworks Model Registry
        self.mr: ModelRegistry = self.project.get_model_registry()

    def _get_football_fgs(self):
        main_fg = self.fs.get_feature_group(
            name=f"football_{self.league.lower()}",
            version=1,
        )

        lags_fg = self.fs.get_feature_group(
            name=f"football_{self.league.lower()}_lags_{self.window_size}",
            version=1,
        )

        return main_fg, lags_fg

    def _load_model(self) -> xgb.XGBClassifier:
        # Loads the model with highest f1_score
        EVALUATION_METRIC = "f1_score"
        SORT_METRICS_BY = "max"  # your sorting criteria
        MODEL_NAME = "football_xgboost"

        # get best model based on custom metrics
        best_model = self.mr.get_best_model(
            MODEL_NAME,
            EVALUATION_METRIC,
            SORT_METRICS_BY,
        )
        model_path = best_model.download("./model")
        xgb_model: xgb.XGBClassifier = joblib.load(
            os.path.join(model_path, "xgboost_model.pkl")
        )

        return xgb_model
