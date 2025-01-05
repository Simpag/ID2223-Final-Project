import pandas as pd

from hsfs.feature_store import FeatureStore
from hsfs.feature_view import FeatureView
from hsfs.hopsworks_udf import udf


FEATURE_VIEW_NAME = "football_train_view"
FEATURE_VIEW_VERSION = 1


def get_feature_view(league: str, window_size: int, fs: FeatureStore) -> FeatureView:
    print("Fetching feature view...")
    try:
        feature_view = fs.get_feature_view(
            name=FEATURE_VIEW_NAME, version=FEATURE_VIEW_VERSION
        )
    except Exception:
        print("Could not fetch feature view, creating a new feature view...")
        feature_view = _create_feature_view(league, window_size, fs)

    print("Fetched feature view")
    return feature_view


def _create_feature_view(
    league: str, window_size: int, fs: FeatureStore
) -> FeatureView:
    main_fg = fs.get_feature_group(
        name=f"football_{league.lower()}",
        version=FEATURE_VIEW_VERSION,
    )

    lags_fg = fs.get_feature_group(
        name=f"football_{league.lower()}_lags_{window_size}",
        version=FEATURE_VIEW_VERSION,
    )

    # Select features for training data
    selected_features = main_fg.select(
        [
            "datetime",
            "hometeam",
            "awayteam",
            "ftour",
            "avgh",
            "avgd",
            "avga",
            "avg_gt_2_5",
            "avg_lt_2_5",
            "league_over_percentage",
            "league_under_percentage",
        ]
    ).join(
        lags_fg.select(
            [
                "hs_lags",
                "as_lags",
                "fthg_lags",
                "ftag_lags",
                "hthg_lags",
                "htag_lags",
                "hst_lags",
                "ast_lags",
            ]
        )
    )
    try:
        label_encoder = fs.get_transformation_function(
            name="ou_transfromation",
            version=1,
        )
    except Exception:
        # Our custom transformation does not exist yet
        print("Creating transfromation function for O/U results")
        @udf(int, drop=["value"], mode="pandas")
        def ou_transformation(value: pd.Series) -> pd.Series:
            return value.apply(
                lambda x: int(x.lower() == "o") if not pd.isna(x) else pd.NA
            )

        label_encoder = fs.create_transformation_function(
            transformation_function=ou_transformation,
            version=1,
        )
        label_encoder.save()

    # Map features to transformations.
    transformation_functions = [
        label_encoder("ftour"),
    ]

    # Get or create the 'transactions_view' feature view
    feature_view = fs.get_or_create_feature_view(
        name=FEATURE_VIEW_NAME,
        version=FEATURE_VIEW_VERSION,
        query=selected_features,
        labels=["ftour"],
        transformation_functions=transformation_functions,
    )

    return feature_view
