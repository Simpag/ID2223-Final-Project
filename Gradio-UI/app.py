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

def get_hist_roi():
    project, fs = login()
    # Initial bank balance, wager amount and which league
    starting_bank = 0
    wager = 1
    league = "E0"

    # Get feature groups
    main_fg = fs.get_feature_group(
        name=f"football_{league.lower()}",
        version=1,
    )

    pred_fg = fs.get_feature_group(
        name=f"football_{league.lower()}_predictions",
        version=1,
    )

    # Query necessary features
    query = pred_fg.select(["datetime", "predictions", "hometeam", "awayteam"]).join(
        main_fg.select(["ftour", "avg_gt_2_5", "avg_lt_2_5"]),
        on=["datetime", "hometeam", "awayteam"],
    )
    df = query.read()

    df = df.sort_values(["datetime", "hometeam", "awayteam"], ignore_index=True)

    # Encode the full time over/under results
    df["ftour_encoded"] = df["ftour"].apply(
        lambda x: int(x.lower() == "o") if not pd.isna(x) else pd.NA
    )

    # Determine the odds based on predictions
    df["odds"] = df.apply(
        lambda row: row["avg_gt_2_5"] if row["predictions"] == 1 else row["avg_lt_2_5"],
        axis=1,
    )

    # Calculate profit/loss for each game
    df["profit"] = df.apply(
        lambda row: (
            wager * (row["odds"] - 1)
            if row["predictions"] == row["ftour_encoded"]
            else -wager
        ),
        axis=1,
    )

    # Calculate cumulative bank balance
    df["bank_balance"] = df["profit"].cumsum() + starting_bank
    df.drop(columns="odds", inplace=True)

    df['date'] = df['datetime'].dt.date
    daily_aggregated = df.groupby('date').agg({
        'profit': 'sum',              # Total profit
        'bank_balance': 'last',       # Last bank balance of the day
        }).reset_index()

    daily_aggregated['date'] = daily_aggregated['date'].astype(str)

    total_best = len(df)
    bets_won = (df['profit'] > 0).sum()
    bets_lost = (df['profit'] < 0).sum()
    current_balance = df.iloc[-1]['bank_balance']
    return {"total_bets": total_best, "bets_won": bets_won, "bets_lost": bets_lost, "current_balance": current_balance, "data": daily_aggregated}

def logout():
    hopsworks.logout()

def get_todays_predictions():
    project, fs = login()
    fg_pred = fs.get_feature_group('football_e0_predictions', version=1)

    # Query the latest row from main_fg based
    main_fg_query = fg_pred.select(
        [
            "datetime",
            "hometeam",
            "awayteam",
            "predictions"
        ]
    ).filter(fg_pred.datetime >= datetime.today().strftime("%Y-%m-%d"))
    main_df = main_fg_query.read(online=False)

    main_df["predictions"] = main_df["predictions"].map(lambda x: "Under" if x == 0 else "Over")
    return main_df


def get_daily_predictions():
    project, fs = login()
    fg_pred = fs.get_feature_group('football_e0_predictions', version=1)

    league = "E0"
      # Get feature groups
    main_fg = fs.get_feature_group(
        name=f"football_{league.lower()}",
        version=1,
    )

    pred_fg = fs.get_feature_group(
        name=f"football_{league.lower()}_predictions",
        version=1,
    )

    # Query necessary features
    query = pred_fg.select(["datetime", "predictions", "hometeam", "awayteam"]).join(
        main_fg.select(["ftour"]),
        on=["datetime", "hometeam", "awayteam"],
    )
    main_df = query.read()



    main_df["predictions"] = main_df["predictions"].map(lambda x: "Under" if x == 0 else "Over")
    main_df["ftour"] = main_df["ftour"].map(lambda x: "Under" if x == "U" else "Over")

    main_df.rename(columns={"ftour": "Result"}, inplace=True)
    return main_df.sort_values(by="datetime", ascending=False).head(10)

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
    
def get_next10games(schedule):
    return schedule.loc[schedule['date'] > today].sort_values(by='date').head(10)

today = datetime.today()

roi = get_hist_roi()


with gr.Blocks() as demo:
    with gr.Row():
        gr.Label(f"Total bets: {roi['total_bets']}")
        gr.Label(f"Bets won: {roi['bets_won']}")
        gr.Label(f"Bets lost: {roi['bets_lost']}")
        gr.Label(f"Current balance: {roi['current_balance']}")
    gr.LinePlot(roi["data"], x="date", y="bank_balance", title="Bank balance over time", y_title="Bank balance", x_title="Date")
    gr.Label("Today's predictions")
    gr.DataFrame(get_todays_predictions,
                 headers=["Date", "Home Team", "Away Team", "Prediction"],
                 every=7200)  # 2hrs
    gr.Label("Last 10 games")
    gr.DataFrame(get_daily_predictions,
                 headers=["Date", "Home Team", "Away Team", "Prediction"],
                 every=7200)  # 2hrs
    
    
    gr.Label("Upcoming games")
    next10 = gr.DataFrame(get_next10games(get_schedule()), 
                         label=None, 
                         headers=["Date", "Home Team", "Away Team"],
                          interactive=False,
                          every=7200)  # 2hrs
                        
demo.launch()