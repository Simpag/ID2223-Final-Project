import os
import joblib
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import xgboost as xgb
from hsfs.feature_view import FeatureView
from hsml.model_registry import ModelRegistry
from sklearn.metrics import confusion_matrix
from sklearn.metrics import f1_score

from src.feature_view import get_feature_view
from src.utils import login, logout


class Trainer:
    def __init__(self, league, window_size, test_size):
        self.league = league
        self.window_size = window_size
        self.test_size = test_size

        self.model_dir = "football_model"
        self.images_dir = os.path.join(self.model_dir, "images")
        self._setup_folders()

    def fit(self):
        self.project, self.fs = login()
        
        print("Retrieving data...")
        X_train, X_test, y_train, y_test, feature_view = self._get_data()
        print("Data retrieved")

        # Create an XGBoost classifier
        clf = xgb.XGBClassifier()

        # Fit XGBoost classifier to the training data
        print("Fitting classifier...")
        clf.fit(X_train, y_train)
        print("Classifier fit")

        print("Saving model...")
        self._save_model(clf, feature_view, X_test, y_test)
        print("Model saved")

        logout()

    def _save_model(self, clf, feature_view, X_test, y_test):
        # Predict the test data using the trained classifier
        y_pred_test = clf.predict(X_test)

        # Save the trained XGBoost classifier to a joblib file in the specified directory
        joblib.dump(clf, os.path.join(self.model_dir, "xgboost_model.pkl"))

        # Create a DataFrame for the confusion matrix results
        results = confusion_matrix(y_test, y_pred_test)
        metrics = {"f1_score": f1_score(y_test, y_pred_test, average="macro")}

        df_cm = pd.DataFrame(
            results,
            ["True Under", "True Over"],
            ["Pred Under", "Pred Over"],
        )

        # Create figure with specific size
        plt.figure(figsize=(8, 6))

        # Create a heatmap using seaborn with annotations
        cm = sns.heatmap(
            df_cm,
            annot=True,
            fmt="d",  # Use integer format for annotations
            cmap="Blues",  # Use a blue colormap
            cbar=True,  # Include a color bar
        )

        # Save the figure
        plt.savefig(f"{self.images_dir}/confusion_matrix.png", bbox_inches="tight")

        # Clear the plot and close the figure
        plt.clf()
        plt.close()

        # Get the model registry
        mr: ModelRegistry = self.project.get_model_registry()

        football_model = mr.python.create_model(
            name="football_xgboost",
            metrics=metrics,
            feature_view=feature_view,
            input_example=X_test.iloc[0],
            description="XGB Classifier for football dataset",
        )

        # Save the model to the specified directory
        football_model.save(self.model_dir)

    def _setup_folders(self):
        # Create directories if they don't exist
        os.makedirs(self.model_dir, exist_ok=True)
        os.makedirs(self.images_dir, exist_ok=True)

    def _get_data(self) -> tuple[
        pd.DataFrame,
        pd.DataFrame,
        pd.DataFrame,
        pd.DataFrame,
    ]:
        feature_view: FeatureView = get_feature_view(
            self.league, self.window_size, self.fs
        )

        X_train, X_test, y_train, y_test = feature_view.train_test_split(
            description="football training dataset",
            test_size=self.test_size,
        )

        # Expand the lags
        X_train = self._expand_lags(X_train)
        X_test = self._expand_lags(X_test)

        # Sort the training features DataFrame 'X_train' based on the 'datetime' column
        X_train = X_train.sort_values("datetime")

        # Reindex the target variable 'y_train' to match the sorted order of 'X_train' index
        y_train = y_train.reindex(X_train.index)

        # Sort the test features DataFrame 'X_test' based on the 'datetime' column
        X_test = X_test.sort_values("datetime")

        # Reindex the target variable 'y_test' to match the sorted order of 'X_test' index
        y_test = y_test.reindex(X_test.index)

        # Drop the 'datetime' column from the training features DataFrame 'X_train'
        X_train.drop(columns=["datetime", "hometeam", "awayteam"], inplace=True)

        # Drop the 'datetime' column from the test features DataFrame 'X_test'
        X_test.drop(columns=["datetime", "hometeam", "awayteam"], inplace=True)

        return X_train, X_test, y_train, y_test, feature_view

    def _expand_lags(self, df: pd.DataFrame):
        # Identify columns with lists
        lag_columns = [col for col in df.columns if col.endswith("_lags")]

        # Expand list columns into separate columns
        for col in lag_columns:
            expanded_cols = pd.DataFrame(df[col].tolist(), index=df.index)
            expanded_cols.columns = [f"{col}_{i+1}" for i in expanded_cols.columns]
            df = pd.concat([df.drop(columns=[col]), expanded_cols], axis=1)

        return df
