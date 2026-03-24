import os
import math
import random
import string
import time
import wandb

os.environ.setdefault("WANDB_BASE_URL", "http://localhost:8080")
os.environ.setdefault("WANDB_API_KEY", "dev-" + "lo" * 20 + "_example")

suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
parent_run_name = f"parent-run-{suffix}"
child_run_names = [
    f"child-run-{suffix}-1",
    f"child-run-{suffix}-2",
]
grandchild_run_name = f"grandchild-run-{suffix}-2a"

# Step 1: Create a parent run with 100 steps
print("=== Creating parent run with 100 steps ===")
run = wandb.init(project="fork-test", name=parent_run_name, config={
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

# Step 2: Fork from the parent at different offsets
forks = [
    (child_run_names[0], 50, 110, 5e-4),
    (child_run_names[1], 75, 220, 2.5e-4),
]
long_child_id = None
long_child_name = None
long_child_fork_step = 150

for child_name, fork_step, total_steps, learning_rate in forks:
    print(f"=== Forking {child_name} from {parent_run_name} ({run.id}) at step {fork_step} ===")
    child_run = wandb.init(
        project="fork-test",
        name=child_name,
        fork_from=f"{run.id}?_step={fork_step}",
        config={
            "learning_rate": learning_rate,
        },
    )

    for step in range(fork_step + 1, total_steps):
        t = step / total_steps
        child_run.log({
            "train/loss": 1.0 * math.exp(-3 * t) + 0.05 + random.gauss(0, 0.01),
            "train/accuracy": 1 - math.exp(-5 * t) + random.gauss(0, 0.005),
            "val/loss": 1.0 * math.exp(-2.5 * t) + 0.08 + random.gauss(0, 0.02),
        }, step=step)

    child_run.finish()
    print(f"{child_name} finished.")

    if child_name == child_run_names[1]:
        long_child_id = child_run.id
        long_child_name = child_name

print("Waiting for WAL flush before forking the longer child...")
time.sleep(3)

print(f"=== Forking {grandchild_run_name} from {long_child_name} ({long_child_id}) at step {long_child_fork_step} ===")
grandchild_run = wandb.init(
    project="fork-test",
    name=grandchild_run_name,
    fork_from=f"{long_child_id}?_step={long_child_fork_step}",
    config={
        "learning_rate": 1.25e-4,
    },
)

for step in range(long_child_fork_step + 1, 280):
    t = step / 280
    grandchild_run.log({
        "train/loss": 0.8 * math.exp(-3.2 * t) + 0.03 + random.gauss(0, 0.008),
        "train/accuracy": 1 - math.exp(-5.5 * t) + random.gauss(0, 0.004),
        "val/loss": 0.9 * math.exp(-2.2 * t) + 0.06 + random.gauss(0, 0.015),
    }, step=step)

grandchild_run.finish()
print(f"{grandchild_run_name} finished.")

print("Done! Check http://localhost:8080 for results.")
