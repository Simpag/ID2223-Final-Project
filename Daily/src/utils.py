import os
import hopsworks
from hsfs.feature_store import FeatureStore


def login(project="ID2223_Project") -> tuple[hopsworks.project.Project, FeatureStore]:
    project = hopsworks.login(
        api_key_value=os.environ["HOPSWORKS_API_KEY"],
        project=project,
    )
    fs = project.get_feature_store()

    return project, fs


def logout():
    hopsworks.logout()
