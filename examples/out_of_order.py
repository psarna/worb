import os
import wandb

os.environ.setdefault("WANDB_BASE_URL", "http://localhost:8080")
os.environ.setdefault("WANDB_API_KEY", "dev-" + 40 * "f")

run = wandb.init(project="test")
run_id = run.id
run.define_metric("*", step_metric="step", step_sync=False)
run.log({"eval_metric": 0.1, "step": 1})
run.log({"eval_metric": 0.3, "step": 3})
run.log({"eval_metric": 0.4, "step": 4})
run.finish()

run = wandb.init(project="test", id=run_id, resume="must")
run.define_metric("*", step_metric="step", step_sync=False)
run.log({"eval_metric": 0.05, "step": 2})
run.finish()

run = wandb.init(project="test", id=run_id, resume="must")
run.define_metric("*", step_metric="step", step_sync=False)
run.log({"eval_metric": 0.02, "step": 2})
run.finish()
