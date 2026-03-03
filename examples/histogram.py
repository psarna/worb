import os
import random
import wandb

os.environ.setdefault("WANDB_BASE_URL", "http://localhost:8080")
os.environ.setdefault("WANDB_API_KEY", "dev-" + "lo" * 20 + "_example")

run = wandb.init(project="test", name="histogram-demo")

for step in range(20):
    predictions = [random.gauss(0, 1) for _ in range(100)]
    run.log({
        "loss": 1.0 / (step + 1),
        "predictions": wandb.Histogram(predictions),
    })

run.finish()
