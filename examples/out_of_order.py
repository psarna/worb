import os
import wandb

os.environ.setdefault("WANDB_BASE_URL", "http://localhost:8080")
os.environ.setdefault("WANDB_API_KEY", "dev-" + "lo" * 20 + "_example")

run = wandb.init(project="test", name="out-of-order-demo")
run_id = run.id
run.define_metric("*", step_metric="step")
run.log({"eval_metric": 0.1, "step": 1})
run.log({"eval_metric": 0.5, "step": 2})
run.log({"eval_metric": 0.3, "step": 4})
run.finish()

run = wandb.init(project="test", id=run_id, resume="must")
run.define_metric("*", step_metric="step")
run.log({"eval_metric": 0.1, "step": 3})
run.finish()

run = wandb.init(project="test", id=run_id, resume="must")
run.define_metric("*", step_metric="step")
run.log({"eval_metric": 0.2, "step": 3})
run.finish()
