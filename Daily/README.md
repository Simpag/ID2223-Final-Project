## Environment Variables
`HOPSWORKS_API_KEY`: Api key to hopsworks must be set in modal secrets
`FOOTBALL_API_KEY`: Api key to Sportsradar must be set in modal secrets

# Run
First cd into this directory.

You can deploy this module by running: `modal deploy start_daily.py --name daily_scheduler`

You can also run it once using: `modal run start_daily.py`

# Info
Over is encoded as 1, under encoded as 0