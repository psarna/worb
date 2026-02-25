import os
import math
import random
import wandb

os.environ["WANDB_BASE_URL"] = "http://localhost:8080"
os.environ["WANDB_API_KEY"] = "dev-" + 40 * "f"

run = wandb.init(project="complex-run", config={
    "learning_rate": 3e-4,
    "batch_size": 64,
    "architecture": "transformer",
    "epochs": 10,
    "hidden_dim": 512,
    "num_layers": 6,
    "dropout": 0.1,
})

num_steps = 5000
for step in range(num_steps):
    t = step / num_steps
    noise = random.gauss(0, 0.02 * (1 - t))

    train_loss = 2.5 * math.exp(-3 * t) + 0.15 + noise
    val_loss = 2.5 * math.exp(-2.8 * t) + 0.2 + random.gauss(0, 0.03)
    accuracy = 1 - math.exp(-4 * t) + random.gauss(0, 0.01)
    accuracy = max(0, min(1, accuracy))
    lr = 3e-4 * (1 + math.cos(math.pi * t)) / 2

    metrics = {
        "train/loss": train_loss,
        "val/loss": val_loss,
        "train/accuracy": accuracy,
        "train/learning_rate": lr,
        "train/perplexity": math.exp(train_loss),
        "train/grad_norm": random.lognormvariate(math.log(0.5) - 2 * t, 0.3),
    }

    if step % 50 == 0:
        weights = [random.gauss(0, math.sqrt(2.0 / 512) * (1 - 0.5 * t)) for _ in range(500)]
        gradients = [random.gauss(0, 0.1 * (1 - t) + 0.001) for _ in range(500)]
        activations = [abs(random.gauss(0, 1)) * (0.5 + 0.5 * t) for _ in range(500)]
        metrics["weights"] = wandb.Histogram(weights)
        metrics["gradients"] = wandb.Histogram(gradients)
        metrics["activations"] = wandb.Histogram(activations)

    if step % 100 == 0:
        layer_norms = {f"layer_{i}": random.lognormvariate(-1 + t, 0.2) for i in range(6)}
        for name, norm in layer_norms.items():
            metrics[f"norms/{name}"] = norm

    run.log(metrics)

run.finish()
