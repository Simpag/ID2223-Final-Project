import modal

from src.predictor import Predictor

image = (
    modal.Image.debian_slim(python_version="3.12")
    .apt_install("git")
    .apt_install("build-essential")
    .pip_install_from_requirements(requirements_txt="modal_container_requirements.txt")
    .add_local_python_source("src")
)
app = modal.App(name="Football XGBoost Model Trainer")


@app.function(
    image=image,
    secrets=[
        modal.Secret.from_name("HOPSWORKS_API_KEY"),
        modal.Secret.from_name("FOOTBALL_API_KEY"),
    ],
    schedule=modal.Cron("0 3 * * *"),  # Every day at 3 am
)
def entry():
    trainer = Predictor(league="E0", window_size=4)
    trainer.predict_and_save()
