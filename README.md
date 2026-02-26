<p align="center">
  <img src="worb.svg" width="100" height="100" alt="worb logo">
</p>

# worb

https://worb.cloud

Local, single-binary server compatible with the standard `wandb` Python client. Also, a bad pun.

- **Powered by DuckDB**: single file, simple backups, built-in SQL console
- **Single executable**: build and run anywhere, just set `WANDB_BASE_URL=http://your-worb-server`
- **built-in GraphQL console**: for the men of culture

## Usage

```bash
./worb
```

```python
import os
import wandb

os.environ["WANDB_DIR"] = "worb"
os.environ["WANDB_BASE_URL"] = "http://localhost:8080"
os.environ["WANDB_API_KEY"] = "dev-"+40*"f"

wandb.init(project="test-project", name="test-run")
for i in range(100):
    wandb.log({"loss": 1.0 / (i + 1), "step": i})
wandb.finish()
```

Then visit `http://localhost:8080` to see the run with metrics charts.

## Examples

Run the example scripts in the `examples/` directory to see how things work:

```bash
WANDB_BASE_URL=http://localhost:8080 python examples/complex.py
WANDB_BASE_URL=http://localhost:8080 python examples/histogram.py
WANDB_BASE_URL=http://localhost:8080 python examples/out_of_order.py
```

## Docker

```bash
docker run -v ~/.worb:/data -p 8080:8080 ghcr.io/psarna/worb --data /data
```

## Build

```bash
go build .
```

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--host` | `127.0.0.1` | Host to bind to (use `0.0.0.0` for all interfaces) |
| `--port` | `8080` | Port to listen on |
| `--data` | `~/.worb` | Data directory for DuckDB and uploaded files |

## Backup

All data lives in a single DuckDB file (`~/.worb/worb.duckdb`). To back it up, just copy the file — no export tools or dump commands needed.

## Demo

![](img/p1.webm)
![](img/p2.png)
![](img/p3.png)
![](img/p4.png)

