import modal

from src.trainer import Trainer

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
    secrets=[modal.Secret.from_name("HOPSWORKS_API_KEY")],
    schedule=modal.Cron("0 2 * * 1"), # Every monday at 2 am
)
def entry():
    trainer = Trainer(league="E0", window_size=7, test_size=0.2)
    trainer.fit()
