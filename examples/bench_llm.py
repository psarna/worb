"""
Benchmark: Simulate MoE-650B LLM training on 10,240 GPUs.

Stress-tests a worb server by logging ~120 metrics every step, with periodic
bursts of per-expert (256), per-layer (384), and per-node/device (7,680)
metrics.  Peak payload: ~8,300+ metric keys in a single wandb.log() call.

Usage:
    go run . serve                              # start worb
    python examples/bench_llm.py --steps 1000   # quick test
    python examples/bench_llm.py                # full 50k-step run
"""

import argparse
import math
import os
import random
import time
from dataclasses import dataclass, field
from datetime import datetime

import wandb


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class Config:
    # server
    base_url: str = ""
    api_key: str = "dev-" + "lo" * 20 + "_bench"

    # model
    total_params_b: float = 650.0
    active_params_b: float = 50.0
    num_experts: int = 128
    top_k: int = 2
    num_layers: int = 128
    hidden_dim: int = 16384
    num_heads: int = 128
    vocab_size: int = 128256

    # parallelism
    tp: int = 8
    pp: int = 16
    ep: int = 16
    gpus_per_node: int = 8
    num_nodes: int = 1280

    # training
    num_steps: int = 0
    global_batch_tokens: int = 13_107_200
    seq_length: int = 32768
    warmup_steps: int = 5000
    max_lr: float = 1.5e-4
    min_lr: float = 1.5e-5

    # logging frequencies
    expert_every: int = 10
    layer_every: int = 50
    device_every: int = 20
    val_every: int = 500
    ckpt_every: int = 1000

    # benchmark
    pp_stages: int = 16
    seed: int = 42
    project: str = ""

    # data sources
    data_sources: tuple = ("web", "code", "books", "academic", "conversation")

    # derived (set in __post_init__)
    total_gpus: int = field(init=False, default=0)
    dp: int = field(init=False, default=0)
    dataset_tokens: float = field(init=False, default=0.0)
    gpu_peak_tflops: float = field(init=False, default=0.0)

    def __post_init__(self):
        self.total_gpus = self.num_nodes * self.gpus_per_node
        self.dp = self.total_gpus // (self.tp * self.pp)
        self.dataset_tokens = self.num_steps * self.global_batch_tokens
        self.gpu_peak_tflops = 989.0  # H100 SXM bf16


# ---------------------------------------------------------------------------
# Learning rate schedule (warmup-stable-decay / WSD)
# ---------------------------------------------------------------------------

def lr_schedule(step: int, cfg: Config) -> float:
    if step < cfg.warmup_steps:
        return cfg.max_lr * step / cfg.warmup_steps
    progress = (step - cfg.warmup_steps) / max(1, cfg.num_steps - cfg.warmup_steps)
    cosine = 0.5 * (1.0 + math.cos(math.pi * progress))
    return cfg.min_lr + (cfg.max_lr - cfg.min_lr) * cosine


# ---------------------------------------------------------------------------
# Metric generators
# ---------------------------------------------------------------------------

def loss_metrics(step: int, cfg: Config, rng: random.Random, state: dict) -> dict:
    t = step / cfg.num_steps
    noise = rng.gauss(0, 0.02 * (1 - t))

    # 1% chance of a loss spike
    spike = 1.5 if rng.random() < 0.01 else 1.0
    state["spike"] = spike

    train_loss = (8.0 * (step + 1) ** (-0.15) + 1.8 + noise) * spike

    m = {
        "loss/train": train_loss,
        "loss/aux": max(0, 0.02 - 0.015 * t + rng.gauss(0, 0.002)),
        "loss/router_z": max(0, 0.05 - 0.04 * t + rng.gauss(0, 0.003)),
        "loss/load_balance": max(0, 0.02 - 0.015 * t + rng.gauss(0, 0.002)),
    }
    # per-source losses
    source_offsets = {"web": 0.0, "code": 0.3, "books": -0.1, "academic": 0.15, "conversation": -0.2}
    for src in cfg.data_sources:
        offset = source_offsets.get(src, 0.0)
        m[f"loss/{src}"] = train_loss + offset + rng.gauss(0, 0.01)
    return m


def throughput_metrics(step: int, cfg: Config, rng: random.Random) -> dict:
    # ramp up during first 100 steps (compilation/warmup)
    ramp = min(1.0, step / 100) if step > 0 else 0.1
    tps = 120 * ramp + rng.gauss(0, 1)  # tokens/sec/device
    tps_global = tps * cfg.total_gpus
    mfu = 40.0 + rng.gauss(0, 0.5)
    hfu = mfu * 1.05
    tflops = tps * cfg.active_params_b * 6 / 1e3  # rough approx
    return {
        "throughput/tps": tps,
        "throughput/tps_global": tps_global,
        "throughput/tflops": tflops,
        "throughput/mfu_pct": mfu,
        "throughput/hfu_pct": hfu,
        "throughput/tokens_per_batch": cfg.global_batch_tokens,
    }


def optimizer_metrics(step: int, cfg: Config, rng: random.Random, state: dict) -> dict:
    t = step / cfg.num_steps
    lr = lr_schedule(step, cfg)
    spike = state.get("spike", 1.0)

    grad_norm = rng.lognormvariate(math.log(1.0) - 0.5 * t, 0.3)
    if spike > 1.0:
        grad_norm *= 3.0

    clip_frac = max(0, 0.05 + 0.25 * math.exp(-3 * t) + rng.gauss(0, 0.01))

    # loss scale dynamics
    ls = state.get("loss_scale", 65536.0)
    if grad_norm > 10.0 and rng.random() < 0.3:
        ls = max(1.0, ls / 2)
        state["num_overflows"] = state.get("num_overflows", 0) + 1
    elif step % 2000 == 0 and step > 0:
        ls = min(65536.0, ls * 2)
    state["loss_scale"] = ls

    return {
        "optimizer/lr": lr,
        "optimizer/lr_progress": t,
        "optimizer/grad_norm": grad_norm,
        "optimizer/clip_fraction": clip_frac,
        "optimizer/loss_scale": ls,
        "optimizer/param_update_norm": rng.lognormvariate(math.log(0.01) - t, 0.2),
        "optimizer/momentum_norm": rng.lognormvariate(math.log(0.5), 0.1),
        "optimizer/variance_norm": rng.lognormvariate(math.log(0.1) - 0.3 * t, 0.15),
    }


def timing_metrics(step: int, cfg: Config, rng: random.Random, state: dict) -> dict:
    step_time = 1.8 + rng.gauss(0, 0.05)  # ~1.8s per step at this scale
    state["wall_time"] = state.get("wall_time", 0.0) + step_time
    data_loading = 0.015 + rng.gauss(0, 0.003)
    if rng.random() < 0.02:
        data_loading += 0.035  # occasional IO spike
    return {
        "time/step_s": step_time,
        "time/total_s": state["wall_time"],
        "time/data_loading_s": max(0, data_loading),
        "time/data_loading_pct": max(0, data_loading / step_time * 100),
        "time/fwd_bwd_s": step_time - data_loading - 0.01,
    }


def data_metrics(step: int, cfg: Config) -> dict:
    tokens_seen = step * cfg.global_batch_tokens
    padding_pct = 3.2  # ~3.2% padding is typical
    return {
        "data/tokens_seen": tokens_seen,
        "data/tokens_target": cfg.dataset_tokens,
        "data/padding_pct": padding_pct,
        "data/mix_web": 0.40,
        "data/mix_code": 0.25,
        "data/mix_books": 0.15,
        "data/mix_academic": 0.12,
        "data/mix_conversation": 0.08,
    }


def moe_scalar_metrics(step: int, cfg: Config, rng: random.Random) -> dict:
    t = step / cfg.num_steps
    return {
        "moe/router_confidence": 0.85 - 0.25 * math.exp(-2 * t) + rng.gauss(0, 0.01),
        "moe/dropped_tokens_pct": max(0, 2.0 * math.exp(-3 * t) + rng.gauss(0, 0.05)),
        "moe/expert_diversity": min(1.0, 1.0 - 0.3 * math.exp(-2 * t) + rng.gauss(0, 0.01)),
        "moe/auxiliary_loss": max(0, 0.015 - 0.01 * t + rng.gauss(0, 0.001)),
    }


def moe_expert_metrics(cfg: Config, rng: random.Random, state: dict) -> dict:
    popularity = state["expert_popularity"]
    m = {}
    total = 0.0
    raw = []
    for i in range(cfg.num_experts):
        u = max(0.001, popularity[i] + rng.gauss(0, 0.15 / cfg.num_experts))
        raw.append(u)
        total += u
    for i in range(cfg.num_experts):
        util = raw[i] / total
        m[f"moe/expert_{i}/utilization"] = util
        m[f"moe/expert_{i}/tokens_routed"] = util * cfg.global_batch_tokens * cfg.top_k
    return m


def layer_metrics(cfg: Config, rng: random.Random, step: int) -> dict:
    t = step / max(1, cfg.num_steps)
    m = {}
    for i in range(cfg.num_layers):
        depth_factor = 1.0 + 0.003 * i
        m[f"layers/{i}/grad_norm"] = rng.lognormvariate(
            math.log(0.5 * depth_factor) - 0.5 * t, 0.25,
        )
        m[f"layers/{i}/weight_norm"] = rng.lognormvariate(
            math.log(1.0 + 0.001 * i), 0.05,
        )
        m[f"layers/{i}/activation_norm"] = rng.lognormvariate(
            math.log(0.5 + 0.003 * i), 0.1,
        )
    return m


def device_metrics(cfg: Config, rng: random.Random, state: dict) -> dict:
    m = {}
    outlier_nodes = state["outlier_nodes"]
    for n in range(cfg.num_nodes):
        is_outlier = n in outlier_nodes
        base_util = 94.0 if not is_outlier else 85.0
        base_temp = 68.0 if not is_outlier else 78.0
        m[f"node/{n}/gpu_util_mean"] = max(0, min(100, base_util + rng.gauss(0, 1.0)))
        m[f"node/{n}/gpu_mem_gb_max"] = 72.0 + rng.gauss(0, 0.5)
        m[f"node/{n}/gpu_temp_max"] = base_temp + rng.gauss(0, 2.0)
        m[f"node/{n}/gpu_power_w_mean"] = 620.0 + rng.gauss(0, 10.0)
        m[f"node/{n}/net_bandwidth_gbps"] = 380.0 + rng.gauss(0, 5.0)
        m[f"node/{n}/ib_errors"] = 1 if (is_outlier and rng.random() < 0.05) else 0
    return m


def pipeline_metrics(cfg: Config, rng: random.Random) -> dict:
    bubble = 1.0 / cfg.pp_stages + rng.gauss(0, 0.005)
    microbatch_ms = 110.0 + rng.gauss(0, 5.0)
    return {
        "pipeline/bubble_frac": max(0, bubble),
        "pipeline/microbatch_ms": microbatch_ms,
        "pipeline/stage_imbalance": max(0, 0.03 + rng.gauss(0, 0.01)),
        "pipeline/comm_ms": 2.5 + rng.gauss(0, 0.3),
        "pipeline/idle_ms": max(0, bubble * microbatch_ms),
    }


def comm_metrics(rng: random.Random) -> dict:
    jitter = 2.0 if rng.random() < 0.02 else 1.0  # 2% chance of 2x jitter
    return {
        "comm/allreduce_ms": (2.5 + rng.gauss(0, 0.3)) * jitter,
        "comm/allgather_ms": (1.8 + rng.gauss(0, 0.2)) * jitter,
        "comm/reduce_scatter_ms": (2.0 + rng.gauss(0, 0.25)) * jitter,
        "comm/all2all_ms": (3.5 + rng.gauss(0, 0.5)) * jitter,
        "comm/bandwidth_gbps": 380.0 + rng.gauss(0, 10.0),
        "comm/latency_us": 1.5 + rng.gauss(0, 0.3),
        "comm/dp_grad_sync_ms": (4.0 + rng.gauss(0, 0.5)) * jitter,
        "comm/pp_send_recv_ms": (0.8 + rng.gauss(0, 0.1)) * jitter,
        "comm/tp_allreduce_ms": (1.2 + rng.gauss(0, 0.15)) * jitter,
        "comm/ep_all2all_ms": (3.0 + rng.gauss(0, 0.4)) * jitter,
    }


def memory_metrics(cfg: Config, rng: random.Random) -> dict:
    m = {
        "memory/max_active_gib": 65.0 + rng.gauss(0, 0.3),
        "memory/max_active_pct": 81.0 + rng.gauss(0, 0.4),
        "memory/max_reserved_gib": 72.0 + rng.gauss(0, 0.3),
        "memory/max_reserved_pct": 90.0 + rng.gauss(0, 0.4),
        "memory/num_alloc_retries": 0,
        "memory/num_ooms": 0,
        "memory/pynvml_max_active_gib": 66.0 + rng.gauss(0, 0.3),
    }
    # per-PP-stage memory (like Titan's multi-rank logging)
    for stage in range(cfg.pp_stages):
        stage_offset = 0.5 * stage / cfg.pp_stages  # later stages use slightly more
        m[f"stage/{stage}/memory/max_active_gib"] = 64.0 + stage_offset + rng.gauss(0, 0.3)
        m[f"stage/{stage}/memory/max_active_pct"] = 80.0 + stage_offset + rng.gauss(0, 0.4)
        m[f"stage/{stage}/memory/max_reserved_gib"] = 71.0 + stage_offset + rng.gauss(0, 0.3)
        m[f"stage/{stage}/memory/max_reserved_pct"] = 89.0 + stage_offset + rng.gauss(0, 0.4)
        m[f"stage/{stage}/memory/num_alloc_retries"] = 0
        m[f"stage/{stage}/memory/num_ooms"] = 0
        m[f"stage/{stage}/memory/pynvml_max_active_gib"] = 65.0 + stage_offset + rng.gauss(0, 0.3)
    return m


def system_metrics(step: int, cfg: Config, rng: random.Random) -> dict:
    m = {
        "system/cpu_util": 28.0 + rng.gauss(0, 3.0),
        "system/ram_gb": 450.0 + rng.gauss(0, 10.0),
        "system/net_rx_gbps": 350.0 + rng.gauss(0, 15.0),
        "system/net_tx_gbps": 350.0 + rng.gauss(0, 15.0),
        "system/ib_errors": 0,
        "system/node_health": min(1.0, 0.995 + rng.gauss(0, 0.003)),
    }
    # checkpoint metrics — non-zero only at checkpoint steps
    if step > 0 and step % cfg.ckpt_every == 0:
        m["system/ckpt_time_sec"] = 120.0 + rng.gauss(0, 10.0)
        m["system/ckpt_size_gb"] = 2400.0
    else:
        m["system/ckpt_time_sec"] = 0.0
        m["system/ckpt_size_gb"] = 0.0
    return m


def validation_metrics(step: int, cfg: Config, rng: random.Random) -> dict:
    t = step / cfg.num_steps
    val_loss = 8.0 * (step + 1) ** (-0.15) + 2.0 + rng.gauss(0, 0.03)
    val_acc = min(0.95, 0.2 + 0.5 * (1 - math.exp(-4 * t)) + rng.gauss(0, 0.005))
    m = {
        "val/loss": val_loss,
        "val/perplexity": math.exp(min(val_loss, 20)),
        "val/accuracy": val_acc,
        "val/top5_acc": min(1.0, val_acc + 0.15 + rng.gauss(0, 0.003)),
        "val/bits_per_byte": val_loss / math.log(2),
        "val/tokens_eval": 1_000_000,
        "val/time_sec": 45.0 + rng.gauss(0, 3.0),
    }
    source_offsets = {"web": 0.0, "code": 0.3, "books": -0.1, "academic": 0.15, "conversation": -0.2}
    for src in cfg.data_sources:
        m[f"val/loss_{src}"] = val_loss + source_offsets.get(src, 0.0) + rng.gauss(0, 0.02)
    return m


# ---------------------------------------------------------------------------
# Step assembly
# ---------------------------------------------------------------------------

def generate_step_metrics(step: int, cfg: Config, rng: random.Random, state: dict) -> dict:
    m = {}
    m.update(loss_metrics(step, cfg, rng, state))
    m.update(throughput_metrics(step, cfg, rng))
    m.update(optimizer_metrics(step, cfg, rng, state))
    m.update(timing_metrics(step, cfg, rng, state))
    m.update(data_metrics(step, cfg))
    m.update(moe_scalar_metrics(step, cfg, rng))
    m.update(pipeline_metrics(cfg, rng))
    m.update(comm_metrics(rng))
    m.update(memory_metrics(cfg, rng))
    m.update(system_metrics(step, cfg, rng))

    if step % cfg.expert_every == 0:
        m.update(moe_expert_metrics(cfg, rng, state))

    if step % cfg.layer_every == 0:
        m.update(layer_metrics(cfg, rng, step))

    if step % cfg.device_every == 0:
        m.update(device_metrics(cfg, rng, state))

    if cfg.val_every > 0 and step > 0 and step % cfg.val_every == 0:
        m.update(validation_metrics(step, cfg, rng))

    return m


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_benchmark(cfg: Config):
    os.environ.setdefault("WANDB_BASE_URL", cfg.base_url)
    os.environ.setdefault("WANDB_API_KEY", cfg.api_key)

    rng = random.Random(cfg.seed ^ int(time.time() * 1000))

    # per-expert popularity bias (seeded once, reused every step)
    expert_popularity = [1.0 / cfg.num_experts * (0.7 + 0.6 * rng.random()) for _ in range(cfg.num_experts)]
    # pick ~2% of nodes as outliers
    outlier_nodes = set(rng.sample(range(cfg.num_nodes), max(1, cfg.num_nodes // 50)))

    state = {
        "loss_scale": 65536.0,
        "num_overflows": 0,
        "wall_time": 0.0,
        "spike": 1.0,
        "expert_popularity": expert_popularity,
        "outlier_nodes": outlier_nodes,
    }

    suffix = datetime.now().strftime("%m%d-%H%M%S")
    run = wandb.init(
        project=cfg.project,
        name=f"moe-650b-10k-gpu-{suffix}",
        tags=["benchmark", "moe", "stress-test"],
        config={
            "model/total_params_b": cfg.total_params_b,
            "model/active_params_b": cfg.active_params_b,
            "model/num_experts": cfg.num_experts,
            "model/top_k": cfg.top_k,
            "model/num_layers": cfg.num_layers,
            "model/hidden_dim": cfg.hidden_dim,
            "model/num_heads": cfg.num_heads,
            "model/vocab_size": cfg.vocab_size,
            "cluster/total_gpus": cfg.total_gpus,
            "cluster/num_nodes": cfg.num_nodes,
            "cluster/gpus_per_node": cfg.gpus_per_node,
            "parallel/tp": cfg.tp,
            "parallel/pp": cfg.pp,
            "parallel/ep": cfg.ep,
            "parallel/dp": cfg.dp,
            "training/num_steps": cfg.num_steps,
            "training/global_batch_tokens": cfg.global_batch_tokens,
            "training/seq_length": cfg.seq_length,
            "training/warmup_steps": cfg.warmup_steps,
            "training/max_lr": cfg.max_lr,
            "training/min_lr": cfg.min_lr,
        },
    )

    total_metrics = 0
    t_start = time.perf_counter()
    last_report = t_start

    for step in range(cfg.num_steps):
        metrics = generate_step_metrics(step, cfg, rng, state)
        total_metrics += len(metrics)
        run.log(metrics, step=step)

        # progress reporting every 10s
        now = time.perf_counter()
        if now - last_report >= 10.0:
            elapsed = now - t_start
            print(
                f"  step {step}/{cfg.num_steps}"
                f"  |  {step / elapsed:.0f} steps/sec"
                f"  |  {total_metrics / elapsed:,.0f} metrics/sec"
                f"  |  {len(metrics)} metrics this step",
            )
            last_report = now

    run.finish()
    t_end = time.perf_counter()

    elapsed = t_end - t_start
    print()
    print("=" * 60)
    print("BENCHMARK RESULTS: worb MoE-650B Stress Test")
    print("=" * 60)
    print(f"Steps:               {cfg.num_steps:,}")
    print(f"Total metrics:       {total_metrics:,}")
    print(f"Wall time:           {elapsed:.2f}s")
    print(f"Steps/sec:           {cfg.num_steps / elapsed:.1f}")
    print(f"Metrics/sec:         {total_metrics / elapsed:,.0f}")
    print(f"Avg metrics/step:    {total_metrics / cfg.num_steps:.0f}")
    print(f"Loss scale overflows:{state.get('num_overflows', 0)}")
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(description="worb benchmark: GPT-4 class training simulation")
    parser.add_argument("--steps", type=int, default=1_000_000)
    parser.add_argument("--base-url", default="http://localhost:8080")
    parser.add_argument("--num-experts", type=int, default=128)
    parser.add_argument("--num-layers", type=int, default=128)
    parser.add_argument("--num-nodes", type=int, default=1280)
    parser.add_argument("--batch-tokens", type=int, default=13_107_200)
    parser.add_argument("--seq-length", type=int, default=32768)
    parser.add_argument("--pp-stages", type=int, default=16)
    parser.add_argument("--expert-every", type=int, default=10)
    parser.add_argument("--layer-every", type=int, default=50)
    parser.add_argument("--device-every", type=int, default=20)
    parser.add_argument("--val-every", type=int, default=500)
    parser.add_argument("--ckpt-every", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--project", default="bench-gpt4-class")
    args = parser.parse_args()

    cfg = Config(
        base_url=args.base_url,
        num_steps=args.steps,
        num_experts=args.num_experts,
        num_layers=args.num_layers,
        num_nodes=args.num_nodes,
        global_batch_tokens=args.batch_tokens,
        seq_length=args.seq_length,
        pp_stages=args.pp_stages,
        expert_every=args.expert_every,
        layer_every=args.layer_every,
        device_every=args.device_every,
        val_every=args.val_every,
        ckpt_every=args.ckpt_every,
        seed=args.seed,
        project=args.project,
    )

    print(f"worb benchmark: GPT-4 class on {cfg.total_gpus} GPUs ({cfg.num_nodes} nodes)")
    print(f"  steps={cfg.num_steps}  batch_tokens={cfg.global_batch_tokens:,}  seq_len={cfg.seq_length}")
    print(f"  total tokens: {cfg.dataset_tokens:.1e}")
    print(f"  experts={cfg.num_experts}  layers={cfg.num_layers}")
    print(f"  device metrics every {cfg.device_every} steps ({cfg.num_nodes * 6:,} keys)")
    print(f"  expert metrics every {cfg.expert_every} steps ({cfg.num_experts * 2:,} keys)")
    print(f"  layer metrics every {cfg.layer_every} steps ({cfg.num_layers * 3:,} keys)")
    print()

    run_benchmark(cfg)


if __name__ == "__main__":
    main()
