import gradio as gr
import hopsworks
import json
from datetime import datetime, timedelta
import pandas as pd
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

def hopsworkstest():
    project, fs = login()
    fg_pred = fs.get_feature_group('football_e0_predictions', version=1)


    print(fg_pred.show(5))

def get_schedule():
    with open('./schedule.json', 'r') as handle:
        parsed = json.load(handle)
        return pd.DataFrame([{
            "date": datetime.strptime(game['sport_event']['start_time'], "%Y-%m-%dT%H:%M:%S+00:00"),
            "home_team": game['sport_event']['competitors'][0]['name'],
            "away_team": game['sport_event']['competitors'][1]['name'],
        }
        for game in parsed["schedules"]
        ])
    
today = datetime.today()

#tomorrow = (datetime.today() + timedelta(1)).strftime('%Y-%m-%d')

schedule = get_schedule()

next10_games = schedule.loc[schedule['date'] > today].sort_values(by='date').head(10)

with gr.Blocks() as demo:
    gr.Label("Next 10 games")
    next10 = gr.DataFrame(next10_games, 
                          label=None, 
                          headers=["Date", "Home Team", "Away Team"],
                          interactive=False
                          )
demo.launch()