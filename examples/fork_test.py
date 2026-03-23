import os
import math
import random
import time
import wandb

os.environ.setdefault("WANDB_BASE_URL", "http://localhost:8080")
os.environ.setdefault("WANDB_API_KEY", "dev-" + "lo" * 20 + "_example")

# Step 1: Create a parent run with 100 steps
print("=== Creating parent run with 100 steps ===")
run = wandb.init(project="fork-test", name="parent-run", config={
    "learning_rate": 1e-3,
    "batch_size": 32,
    "model": "resnet50",
})

for step in range(100):
    t = step / 100
    run.log({
        "train/loss": 2.0 * math.exp(-3 * t) + 0.1 + random.gauss(0, 0.02),
        "train/accuracy": 1 - math.exp(-4 * t) + random.gauss(0, 0.01),
        "val/loss": 2.0 * math.exp(-2.5 * t) + 0.15 + random.gauss(0, 0.03),
    }, step=step)

run.finish()
print("Parent run finished.")

# Wait for WAL to flush
print("Waiting for WAL flush...")
time.sleep(3)

# Step 2: Fork from step 50
print("=== Forking from parent-run at step 50 ===")
run2 = wandb.init(project="fork-test", fork_from="parent-run?_step=50", config={
    "learning_rate": 5e-4,  # override the learning rate
})

for step in range(51, 150):
    t = step / 150
    run2.log({
        "train/loss": 1.0 * math.exp(-3 * t) + 0.05 + random.gauss(0, 0.01),
        "train/accuracy": 1 - math.exp(-5 * t) + random.gauss(0, 0.005),
        "val/loss": 1.0 * math.exp(-2.5 * t) + 0.08 + random.gauss(0, 0.02),
    }, step=step)

run2.finish()
print("Forked run finished.")
print("Done! Check http://localhost:8080 for results.")
