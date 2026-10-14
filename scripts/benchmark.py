"""Measured benchmark of the consumer pipeline (spec section 15).

For every run the script:
  1. stops the consumers, wipes tables / Redis / topics and recreates the topics,
  2. pre-loads N messages with the simulator (``--count N --rate 0``) while no consumer is running,
  3. starts K consumers (BATCH_SIZE=b) and waits until all N rows are in PostgreSQL,
  4. records throughput, latency percentiles and resource usage.

Throughput = N / (last processed_at - first processed_at), i.e. the time the consumers needed to
drain the backlog, excluding container start-up.  Run from the repo root with the stack built:

    python scripts/benchmark.py --messages 200000 --runs 3 --experiments A B C D E
    python scripts/benchmark.py --render-only        # regenerate docs/BENCHMARKS.md from results

Every number written to docs/BENCHMARKS.md comes from benchmarks/results.json.
"""

import argparse
import json
import platform
import re
import statistics
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "benchmarks" / "results.json"
DOC = ROOT / "docs" / "BENCHMARKS.md"
KAFKA_BIN = "/opt/kafka/bin"
GROUP = "txn-processors"
BEST_BATCH = 200


@dataclass(frozen=True)
class Experiment:
    id: str
    consumers: int
    batch: int | None  # None = best batch size found by A/B
    partitions: int
    purpose: str


EXPERIMENTS = {
    "A": Experiment("A", 1, 1, 6, "baseline: 1 consumer, per-message"),
    "B": Experiment("B", 1, BEST_BATCH, 6, "1 consumer, batched inserts and commits"),
    "C": Experiment("C", 3, None, 6, "3 consumers, best batch size"),
    "D": Experiment("D", 6, None, 6, "6 consumers (= partitions), best batch size"),
    "E": Experiment("E", 8, None, 6, "8 consumers (> partitions): idle consumers"),
    "F": Experiment("F", 6, None, 3, "6 consumers on 3 partitions"),
}


def run(cmd: list[str], *, env: dict[str, str] | None = None, check: bool = True) -> str:
    import os

    full_env = {**os.environ, **(env or {})}
    proc = subprocess.run(
        cmd, cwd=ROOT, env=full_env, capture_output=True, text=True, encoding="utf-8"
    )
    if check and proc.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd)} failed:\n{proc.stdout}\n{proc.stderr}")
    return proc.stdout


def compose(*args: str, env: dict[str, str] | None = None, check: bool = True) -> str:
    return run(["docker", "compose", *args], env=env, check=check)


def kafka_exec(tool: str, *args: str, check: bool = True) -> str:
    return compose(
        "exec", "-T", "kafka", f"{KAFKA_BIN}/{tool}", "--bootstrap-server", "localhost:9092",
        *args, check=check,
    )  # fmt: skip


def psql(sql: str) -> str:
    return compose("exec", "-T", "postgres", "psql", "-U", "txn", "-tA", "-c", sql).strip()


def reset(partitions: int) -> None:
    compose("stop", "consumer")
    compose("rm", "-f", "consumer", check=False)
    psql("TRUNCATE alerts, transactions RESTART IDENTITY CASCADE")
    compose("exec", "-T", "redis", "redis-cli", "flushall")
    kafka_exec("kafka-consumer-groups.sh", "--delete", "--group", GROUP, check=False)
    for topic in ("transactions.raw", "transactions.dlq"):
        kafka_exec("kafka-topics.sh", "--delete", "--topic", topic, check=False)
    deadline = time.time() + 60
    while time.time() < deadline:
        if "transactions" not in kafka_exec("kafka-topics.sh", "--list"):
            break
        time.sleep(1)
    for _ in range(10):  # topic deletion is asynchronous; retry until creation succeeds
        out = compose(
            "run", "--rm", "kafka-init", env={"RAW_PARTITIONS": str(partitions)}, check=False
        )
        parts = kafka_exec("kafka-topics.sh", "--describe", "--topic", "transactions.raw")
        if f"PartitionCount: {partitions}" in parts:
            return
        time.sleep(2)
    raise RuntimeError(f"could not create topics with {partitions} partitions:\n{out}")


def start_consumers(n: int, batch: int) -> None:
    compose(
        "up", "-d", "--scale", f"consumer={n}", "--force-recreate", "--no-deps", "consumer",
        env={"BATCH_SIZE": str(batch)},
    )  # fmt: skip
    deadline = time.time() + 120
    while time.time() < deadline:
        ps = compose("ps", "consumer", "--format", "{{.Status}}")
        lines = [ln for ln in ps.splitlines() if ln.strip()]
        if len(lines) == n and all("healthy" in ln and "unhealthy" not in ln for ln in lines):
            return
        time.sleep(2)
    raise RuntimeError("consumers did not become healthy")


def sample_cpu(peaks: dict[str, float]) -> None:
    out = run(["docker", "stats", "--no-stream", "--format", "{{.Name}} {{.CPUPerc}}"], check=False)
    for line in out.splitlines():
        name, _, pct = line.partition(" ")
        m = re.match(r"txn-pipeline-([a-z-]+?)-\d+$", name)
        if m and pct.endswith("%"):
            svc = m.group(1)
            peaks[svc] = max(peaks.get(svc, 0.0), float(pct.rstrip("%")))


def prom_query(expr: str) -> float | None:
    url = "http://localhost:9090/api/v1/query?" + urllib.parse.urlencode({"query": expr})
    try:
        result = json.load(urllib.request.urlopen(url, timeout=10))["data"]["result"]
        return float(result[0]["value"][1]) if result else None
    except Exception:
        return None


@dataclass
class RunResult:
    experiment: str
    run: int
    consumers: int
    batch_size: int
    partitions: int
    messages: int
    rows: int
    dlq: int
    throughput_msg_s: float
    drain_seconds: float
    wall_seconds: float
    proc_ms_p50: float | None
    proc_ms_p95: float | None
    proc_ms_p99: float | None
    e2e_s_p50: float
    e2e_s_p95: float
    e2e_s_p99: float
    retries: float | None
    peak_cpu_pct: dict[str, float]
    finished_at: str


def execute(exp: Experiment, run_no: int, n: int, batch: int, timeout: int) -> RunResult:
    print(
        f"[{exp.id}#{run_no}] consumers={exp.consumers} batch={batch} partitions={exp.partitions}"
    )
    reset(exp.partitions)
    compose(
        "--profile", "load", "run", "--rm", "simulator",
        "--count", str(n), "--rate", "0", "--seed", "42", "--accounts", "2000",
    )  # fmt: skip
    wall_start = time.time()
    start_consumers(exp.consumers, batch)
    peaks: dict[str, float] = {}
    deadline = time.time() + timeout
    rows, last_change, last_rows = 0, time.time(), -1
    while time.time() < deadline:
        rows = int(psql("SELECT count(*) FROM transactions"))
        if rows >= n:
            break
        if rows != last_rows:
            last_rows, last_change = rows, time.time()
        elif time.time() - last_change > 120:
            raise RuntimeError(f"no progress for 120s at {rows}/{n} rows")
        sample_cpu(peaks)
        time.sleep(2)
    else:
        raise RuntimeError(f"timeout at {rows}/{n} rows")
    wall = time.time() - wall_start

    span, p50, p95, p99 = (
        float(x)
        for x in psql(
            "SELECT extract(epoch FROM max(processed_at) - min(processed_at)),"
            " percentile_cont(ARRAY[0.5,0.95,0.99]) WITHIN GROUP"
            " (ORDER BY extract(epoch FROM processed_at - event_time)) FROM transactions"
        )
        .replace("{", "")
        .replace("}", "")
        .replace("|", ",")
        .split(",")
    )
    dlq = sum(
        int(line.rsplit(":", 1)[1])
        for line in kafka_exec(
            "kafka-get-offsets.sh", "--topic", "transactions.dlq", check=False
        ).splitlines()
        if ":" in line
    )
    time.sleep(7)  # let Prometheus scrape the final counters
    window = f"{int(wall) + 20}s"
    hist = "txn_processing_seconds_bucket"
    proc = [
        prom_query(f"histogram_quantile({q}, sum by (le) (increase({hist}[{window}])))")
        for q in (0.5, 0.95, 0.99)
    ]
    retries = prom_query(f"sum(increase(txn_retries_total[{window}]))")
    return RunResult(
        experiment=exp.id,
        run=run_no,
        consumers=exp.consumers,
        batch_size=batch,
        partitions=exp.partitions,
        messages=n,
        rows=rows,
        dlq=dlq,
        throughput_msg_s=round(n / span, 1) if span > 0 else 0.0,
        drain_seconds=round(span, 2),
        wall_seconds=round(wall, 2),
        proc_ms_p50=None if proc[0] is None else round(proc[0] * 1000, 3),
        proc_ms_p95=None if proc[1] is None else round(proc[1] * 1000, 3),
        proc_ms_p99=None if proc[2] is None else round(proc[2] * 1000, 3),
        e2e_s_p50=round(p50, 2),
        e2e_s_p95=round(p95, 2),
        e2e_s_p99=round(p99, 2),
        retries=retries,
        peak_cpu_pct={k: round(v, 1) for k, v in sorted(peaks.items())},
        finished_at=datetime.now(UTC).isoformat(timespec="seconds"),
    )


def machine() -> dict[str, str]:
    docker = run(["docker", "info", "--format", "{{.NCPU}} CPUs, {{.MemTotal}} bytes"], check=False)
    return {
        "os": platform.platform(),
        "cpu": platform.processor(),
        "host_cpu_count": str(__import__("os").cpu_count()),
        "docker_vm": docker.strip(),
        "python": platform.python_version(),
    }


def load() -> dict:
    if RESULTS.exists():
        return json.loads(RESULTS.read_text())
    return {"machine": {}, "runs": []}


def render(data: dict) -> None:
    by_exp: dict[str, list[dict]] = {}
    for r in data["runs"]:
        by_exp.setdefault(r["experiment"], []).append(r)
    rows = []
    for exp_id in sorted(by_exp):
        rs = by_exp[exp_id]
        med = statistics.median
        first = rs[0]
        e = EXPERIMENTS[exp_id]
        proc = [r["proc_ms_p95"] for r in rs if r["proc_ms_p95"] is not None]
        rows.append(
            f"| {exp_id} | {first['consumers']} | {first['batch_size']} | {first['partitions']} "
            f"| {len(rs)} | {med(r['throughput_msg_s'] for r in rs):,.0f} "
            f"| {med(r['drain_seconds'] for r in rs):.1f} "
            f"| {f'{med(proc):.2f}' if proc else 'n/a'} "
            f"| {med(r['e2e_s_p50'] for r in rs):.1f} / {med(r['e2e_s_p95'] for r in rs):.1f} / "
            f"{med(r['e2e_s_p99'] for r in rs):.1f} "
            f"| {sum(r['dlq'] for r in rs)} | {e.purpose} |"
        )
    cpu_lines = []
    for exp_id in sorted(by_exp):
        peaks: dict[str, float] = {}
        for r in by_exp[exp_id]:
            for svc, v in r["peak_cpu_pct"].items():
                peaks[svc] = max(peaks.get(svc, 0.0), v)
        cpu_lines.append(
            f"| {exp_id} | " + ", ".join(f"{k} {v:.0f}%" for k, v in peaks.items()) + " |"
        )
    n = by_exp[next(iter(by_exp))][0]["messages"] if by_exp else 0
    m = data.get("machine", {})
    block = "\n".join(
        [
            "<!-- RESULTS:START (generated by scripts/benchmark.py --render-only) -->",
            f"Messages per run: **{n:,}**. Values are the **median** over the runs listed.",
            "",
            "| ID | Consumers | Batch | Partitions | Runs | Throughput (msg/s) | Drain time (s) "
            "| Processing p95 (ms/msg) | End-to-end p50 / p95 / p99 (s) | DLQ | Purpose |",
            "|---|---|---|---|---|---|---|---|---|---|---|",
            *rows,
            "",
            "Peak CPU% seen by `docker stats` (sampled every ~2s; 100% = one core):",
            "",
            "| ID | Peak CPU per service |",
            "|---|---|",
            *cpu_lines,
            "",
            "Test machine: " + "; ".join(f"{k}: {v}" for k, v in m.items()),
            "<!-- RESULTS:END -->",
        ]
    )
    text = DOC.read_text(encoding="utf-8")
    new = re.sub(r"<!-- RESULTS:START.*?<!-- RESULTS:END -->", lambda _: block, text, flags=re.S)
    DOC.write_text(new, encoding="utf-8")
    print(f"updated {DOC}")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--messages", type=int, default=200_000)
    p.add_argument("--runs", type=int, default=3)
    p.add_argument("--experiments", nargs="+", default=["A", "B", "C", "D", "E"])
    p.add_argument("--best-batch", type=int, default=None, help="override batch for C/D/E/F")
    p.add_argument("--timeout", type=int, default=3600, help="per-run timeout in seconds")
    p.add_argument("--render-only", action="store_true")
    p.add_argument("--no-render", action="store_true", help="do not touch docs/BENCHMARKS.md")
    p.add_argument("--fresh", action="store_true", help="discard earlier results")
    args = p.parse_args()

    data = {"machine": {}, "runs": []} if args.fresh else load()
    if args.render_only:
        render(data)
        return 0

    compose("up", "-d", "--build", "--wait", "kafka", "postgres", "redis", "prometheus")
    compose("run", "--rm", "migrate")
    data["machine"] = machine()
    best = args.best_batch or BEST_BATCH
    for exp_id in args.experiments:
        exp = EXPERIMENTS[exp_id]
        batch = exp.batch if exp.batch is not None else best
        data["runs"] = [r for r in data["runs"] if r["experiment"] != exp_id]
        for i in range(1, args.runs + 1):
            result = execute(exp, i, args.messages, batch, args.timeout)
            data["runs"].append(asdict(result))
            RESULTS.parent.mkdir(exist_ok=True)
            RESULTS.write_text(json.dumps(data, indent=2))
            print(f"    -> {result.throughput_msg_s:,.0f} msg/s in {result.drain_seconds}s")
    compose("stop", "consumer", check=False)
    if not args.no_render:
        render(data)
    return 0


if __name__ == "__main__":
    sys.exit(main())
