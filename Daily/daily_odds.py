import json
from datetime import timedelta, datetime
import requests
import os

# Local testing
def get_odds_local():
    with open('./daily/daily2.json', 'r') as handle:
        parsed = json.load(handle)
     
        return parsed

def get_odds(date):
    api_key = os.getenv("FOOTBALL_API_KEY", "") 
    url = f"https://api.sportradar.com/oddscomparison-ust1/en/eu/sports/sr%3Asport%3A1/{date}/schedule.json?api_key={api_key}"


    headers = {"accept": "application/json"}

    try:
    # Send a GET request
        response = requests.get(url, headers=headers)

        # Check if the request was successful
        if response.status_code == 200:
            # Parse the JSON response
            data = response.json()
            #print("Fetched Data:", json.dumps(data, indent=4))
            return data 
        else:
            print(f"Failed to fetch data. Status code: {response.status_code}")
            return []

    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")

# Get only our selected league
def filter_odds(odds):
    # sr:season:118689 = Premier league Season 24/25
    return list(filter(lambda x: x['season']['id'] == "sr:season:118689", odds["sport_events"]))

def extract_features(odds):
    games = []
    for game in odds:

        home = ""
        away = ""
        #extract home and away team
        for team in game["competitors"]:
            if team["qualifier"] == "home":
                home = team["name"]
            else:
                away = team["name"]

        #print(f"{home} vs {away}")

        # extract odds
        home_odds = ""
        draw_odds = ""
        away_odds = ""

        over25 = ""
        under25 = ""

        for market in game["markets"]:
            match market["name"]:
                case "3way":
                    # just get first one
                    for x in market["books"][0]["outcomes"]:
                        match x["type"]:
                            case "home":
                                home_odds = x["odds"]
                            case "draw":
                                draw_odds = x["odds"]
                            case "away":
                                away_odds = x["odds"]
                            

                case _:
                    pass

        #print(f"odds: home: {home_odds}, draw: {draw_odds}, away: {away_odds}")


        for y in game["consensus"]["lines"]:
            if y["name"] == "total_current":
                for outcome in y["outcomes"]:
                    if outcome["type"] == "over":
                        over25 = outcome["odds"]
                    else:
                        under25 = outcome["odds"]
        #print(f"over: {over25}, under: {under25}")

        games.append({
            "home": translate_table[home],
            "away": translate_table[away],
            "home_odds": home_odds,
            "draw_odds": draw_odds,
            "away_odds": away_odds,
            "over25": over25,
            "under25": under25
        })


    return games

translate_table = {
    "Manchester United": "Man United",
    "Ipswich Town": "Ipswich",
    "Arsenal FC": "Arsenal",
    "Everton FC": "Everton",
    "Newcastle United": "Newcastle",
    "Nottingham Forest": "Nott'm Forest",
    "West Ham United": "West Ham",
    "Brentford FC": "Brentford",
    "Chelsea FC": "Chelsea",
    "Leicester City": "Leicester",
    "Brighton & Hove Albion": "Brighton",
    "Crystal Palace": "Crystal Palace",
    "Fulham FC": "Fulham",
    "Manchester City": "Man City",
    "Southampton FC": "Southampton",
    "Tottenham Hotspur": "Tottenham",
    "Aston Villa": "Aston Villa",
    "AFC Bournemouth": "Bournemouth",
    "Wolverhampton Wanderers": "Wolves",
    "Liverpool FC": "Liverpool"
}

def main():
    today = datetime.today().strftime('%Y-%m-%d')
    #tomorrow = (datetime.today() + timedelta(1)).strftime('%Y-%m-%d')

    # Get odds for all games today
    odds = get_odds(today)
    #odds = get_odds_local()

    # Filter to only premier league
    premier_odds = filter_odds(odds)

    # Quit if there are no games today
    if len(premier_odds) == 0:
        print("No Games found in Premier League today")
        quit()

    # Get neede features for inference
    games = extract_features(premier_odds)
    print("Games:", json.dumps(games, indent=4))

if __name__ == "__main__":
    main()