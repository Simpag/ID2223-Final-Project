import modal

from src.data_ingestion import run

image = (
    modal.Image.debian_slim(python_version="3.12")
    .apt_install("git")
    .apt_install("build-essential")
    .pip_install_from_requirements(requirements_txt="modal_container_requirements.txt")
    .add_local_python_source("src")
    .add_local_file("./config.yaml", "/root/config.yaml")
)
app = modal.App(name="Football Data Ingestor")


@app.function(
    image=image,
    secrets=[modal.Secret.from_name("HOPSWORKS_API_KEY")],
    schedule=modal.Cron("0 1 * * *"),  # Every day at 1 am
)
def entry():
    run("/root/config.yaml")
