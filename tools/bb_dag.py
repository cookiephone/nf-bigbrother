#!/usr/bin/env python3
"""Draw the physical execution graph from a BigBrother snapshot or event log.

Takes either a JSON snapshot or an `events_*.jsonl` event log and produces a
Graphviz DOT graph (one node per task, edges from the file dependencies),
rendering it to SVG/PNG/PDF if the `dot` binary is around. --watch keeps
re-rendering the newest output in a directory, which is handy while a run is
going -- the event log is appended to after every task, so it stays current.

    bb_dag.py bb_out/complete_*.json                  # dot to stdout
    bb_dag.py bb_out/events_*.jsonl -f svg -o graph.svg
    bb_dag.py snap.json -f svg --metrics -o graph.svg
    bb_dag.py --watch bb_out -f svg -o live.svg
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import re
import shutil
import subprocess
import sys
import time

# A colour-blind-friendly qualitative palette (ColorBrewer Set3 + Paired).
PALETTE = [
    "#8dd3c7", "#ffffb3", "#bebada", "#fb8072", "#80b1d3", "#fdb462",
    "#b3de69", "#fccde5", "#d9d9d9", "#bc80bd", "#ccebc5", "#ffed6f",
    "#a6cee3", "#1f78b4", "#b2df8a", "#33a02c", "#fb9a99", "#e31a1c",
]


def short_name(full_name: str) -> str:
    """Drop the qualified prefix and the sample suffix from a process name."""
    if not full_name:
        return "task"
    base = re.sub(r"\s*\(.*\)\s*$", "", full_name)
    return base.rsplit(":", 1)[-1] or base


def sample_tag(full_name: str) -> str:
    """The parenthetical tag of a task name, e.g. 'sampleB', or ''."""
    m = re.search(r"\(([^()]*)\)\s*$", full_name or "")
    return m.group(1) if m else ""


def human_bytes(n: int) -> str:
    step = 1024.0
    value = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < step:
            return f"{value:.0f}{unit}" if unit == "B" else f"{value:.1f}{unit}"
        value /= step
    return f"{value:.1f}PB"


def infer_edges(tasks: dict) -> None:
    """Fill in parents/children from the file lists, as the plugin does.

    A is a parent of B when an output file of A is an input file of B. Needed
    when replaying an event log, whose task lines carry the files but not the
    children -- a task's children are not known at the moment it finishes.
    """
    producers: dict[str, list[str]] = {}
    for tid, task in tasks.items():
        for path in task.get("outputFiles") or []:
            producers.setdefault(path, []).append(tid)

    parents: dict[str, set] = {tid: set() for tid in tasks}
    children: dict[str, set] = {tid: set() for tid in tasks}
    for tid, task in tasks.items():
        for path in task.get("inputFiles") or []:
            for producer in producers.get(path, ()):
                if producer != tid:
                    parents[tid].add(producer)
                    children[producer].add(tid)

    for tid, task in tasks.items():
        task["parents"] = sorted(parents[tid])
        task["children"] = sorted(children[tid])


def count_task_events(path: str) -> int:
    """How many task lines an event log holds, i.e. how many frames it can make."""
    total = 0
    with open(path) as fh:
        for line in fh:
            if '"event":"task"' in line or '"event": "task"' in line:
                total += 1
    return total


def replay_events(path: str, upto: int | None = None) -> dict:
    """Replay an events_*.jsonl into the shape a JSON snapshot has.

    `upto` stops after that many task lines, which is what makes an event log a
    drop-in replacement for the partial_* snapshot sequence.
    """
    name = "workflow"
    tasks: dict[str, dict] = {}
    executions: dict[str, dict] = {}
    files: list = []
    machines: list = []
    makespan = ""
    seen = 0

    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue  # a partially flushed final line, while a run is live
            kind = rec.get("event")
            if kind == "run":
                name = rec.get("name") or name
            elif kind == "task":
                if upto is not None and seen >= upto:
                    break
                seen += 1
                tid = rec["id"]
                tasks[tid] = {
                    "id": tid,
                    "name": rec.get("name", ""),
                    "inputFiles": rec.get("inputFiles") or [],
                    "outputFiles": rec.get("outputFiles") or [],
                    "parents": [],
                    "children": [],
                }
                if rec.get("execution"):
                    executions[tid] = rec["execution"]
            elif kind in ("complete", "error"):
                files = rec.get("files") or []
                machines = rec.get("machines") or []
                makespan = rec.get("makespanInSeconds", "")

    infer_edges(tasks)
    return {
        "name": name,
        "workflow": {
            "specification": {"tasks": list(tasks.values()), "files": files},
            "execution": {
                "tasks": [dict(ex, id=tid) for tid, ex in executions.items()],
                "machines": machines,
                "makespanInSeconds": makespan,
            },
        },
    }


class Snapshot:
    """Parsed view of a BigBrother JSON snapshot."""

    def __init__(self, data: dict):
        wf = data.get("workflow", {})
        spec = wf.get("specification", {})
        execu = wf.get("execution", {})
        self.name = data.get("name", "workflow")
        self.tasks = {t["id"]: t for t in spec.get("tasks", [])}
        self.executions = {t["id"]: t for t in execu.get("tasks", [])}
        self.makespan = execu.get("makespanInSeconds", "")

    @classmethod
    def load(cls, path: str) -> "Snapshot":
        """Load a JSON snapshot, or an events_*.jsonl event log."""
        if path.endswith(".jsonl"):
            return cls(replay_events(path))
        with open(path) as fh:
            return cls(json.load(fh))

    @classmethod
    def from_events(cls, path: str, upto: int | None = None) -> "Snapshot":
        """The graph as it stood after `upto` tasks had finished."""
        return cls(replay_events(path, upto))

    def edges(self, completed_only: bool) -> list[tuple[str, str]]:
        keep = self._kept_ids(completed_only)
        seen = set()
        result = []
        for tid in keep:
            for child in self.tasks[tid].get("children", []):
                if child in keep and (tid, child) not in seen:
                    seen.add((tid, child))
                    result.append((tid, child))
        return result

    def _kept_ids(self, completed_only: bool) -> set:
        if not completed_only:
            return set(self.tasks)
        return {tid for tid in self.tasks if tid in self.executions}

    def node_label(self, tid: str, metrics: bool) -> str:
        task = self.tasks[tid]
        lines = [short_name(task.get("name", ""))]
        tag = sample_tag(task.get("name", ""))
        if tag:
            lines.append(tag)
        lines.append(f"#{tid}")
        if metrics and tid in self.executions:
            ex = self.executions[tid]
            rt = ex.get("runtimeInSeconds")
            if rt is not None:
                lines.append(f"{rt:.2f}s")
            rss = ex.get("peak_rss") or ex.get("memoryInBytes") or 0
            if rss:
                lines.append(human_bytes(int(rss)))
        return "\\n".join(_dot_escape(x) for x in lines)


def _dot_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def build_dot(snap: Snapshot, *, cluster: bool, completed_only: bool,
              metrics: bool, rankdir: str, label_files: bool) -> str:
    keep = snap._kept_ids(completed_only)
    colour_of: dict[str, str] = {}

    def colour(proc: str) -> str:
        if proc not in colour_of:
            colour_of[proc] = PALETTE[len(colour_of) % len(PALETTE)]
        return colour_of[proc]

    out = ["digraph physical {"]
    out.append(f'  rankdir={rankdir};')
    out.append('  labelloc="t";')
    out.append(f'  label="{_dot_escape(snap.name)} — physical execution graph '
               f'({len(keep)} tasks)";')
    out.append('  fontname="Helvetica"; fontsize=14;')
    out.append('  node [shape=box, style="rounded,filled", '
               'fontname="Helvetica", fontsize=10];')
    out.append('  edge [color="#555555", arrowsize=0.7];')

    # group task ids by process short name
    groups: dict[str, list[str]] = {}
    for tid in keep:
        groups.setdefault(short_name(snap.tasks[tid].get("name", "")), []).append(tid)

    def emit_node(tid: str, indent: str) -> str:
        proc = short_name(snap.tasks[tid].get("name", ""))
        return (f'{indent}"t{tid}" [label="{snap.node_label(tid, metrics)}", '
                f'fillcolor="{colour(proc)}"];')

    for proc in sorted(groups):
        ids = sorted(groups[proc], key=lambda x: int(x) if x.isdigit() else x)
        if cluster and len(ids) > 1:
            safe = re.sub(r"[^A-Za-z0-9_]", "_", proc)
            out.append(f'  subgraph "cluster_{safe}" {{')
            out.append(f'    label="{_dot_escape(proc)}"; style="rounded,dashed"; '
                       f'color="#999999"; fontsize=11;')
            for tid in ids:
                out.append(emit_node(tid, "    "))
            out.append("  }")
        else:
            for tid in ids:
                out.append(emit_node(tid, "  "))

    for parent, child in snap.edges(completed_only):
        attr = ""
        if label_files:
            shared = set(snap.tasks[parent].get("outputFiles", [])) & \
                     set(snap.tasks[child].get("inputFiles", []))
            if shared:
                attr = f' [label=" {len(shared)}", fontsize=8, fontcolor="#777777"]'
        out.append(f'  "t{parent}" -> "t{child}"{attr};')

    out.append("}")
    return "\n".join(out) + "\n"


def render(dot_text: str, out_path: str, fmt: str) -> None:
    if fmt == "dot":
        if out_path == "-":
            sys.stdout.write(dot_text)
        else:
            with open(out_path, "w") as fh:
                fh.write(dot_text)
        return
    dot_bin = shutil.which("dot")
    if not dot_bin:
        sys.exit("error: the 'dot' binary (graphviz) is required to render "
                 f"{fmt}; install graphviz or use -f dot")
    proc = subprocess.run([dot_bin, f"-T{fmt}", "-o", out_path],
                          input=dot_text.encode(), capture_output=True)
    if proc.returncode != 0:
        sys.exit(f"error: dot failed: {proc.stderr.decode().strip()}")


def default_output(input_path: str, fmt: str) -> str:
    if fmt == "dot":
        return "-"
    base = re.sub(r"\.jsonl?$", "", input_path)
    return f"{base}.{fmt}"


def newest_snapshot(directory: str) -> str | None:
    """The most recently written thing worth rendering in a run's output dir.

    The event log counts: it is appended to after every task, so watching it
    gives the same live view that partial snapshots used to, without the run
    having to rewrite the whole graph each time.
    """
    candidates = glob.glob(os.path.join(directory, "events_*.jsonl")) + \
                 glob.glob(os.path.join(directory, "partial_*.json")) + \
                 glob.glob(os.path.join(directory, "complete_*.json")) + \
                 glob.glob(os.path.join(directory, "error_*.json"))
    if not candidates:
        return None
    return max(candidates, key=os.path.getmtime)


def render_one(path: str, args) -> None:
    snap = Snapshot.load(path)
    dot_text = build_dot(
        snap, cluster=not args.no_cluster, completed_only=args.completed_only,
        metrics=args.metrics, rankdir=args.rankdir, label_files=args.label_files)
    out_path = args.output or default_output(path, args.format)
    render(dot_text, out_path, args.format)
    if out_path != "-":
        print(f"{os.path.basename(path)} -> {out_path} "
              f"({len(snap._kept_ids(args.completed_only))} tasks)", file=sys.stderr)


def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        description="Render the physical execution graph from a BigBrother JSON snapshot.")
    p.add_argument("input", nargs="?", help="path to a BigBrother JSON snapshot")
    p.add_argument("-f", "--format", default="dot",
                   choices=["dot", "svg", "png", "pdf"],
                   help="output format (default: dot to stdout)")
    p.add_argument("-o", "--output", help="output path ('-' for stdout)")
    p.add_argument("--no-cluster", action="store_true",
                   help="do not group task nodes by process")
    p.add_argument("--completed-only", action="store_true",
                   help="only include tasks that have finished executing")
    p.add_argument("--metrics", action="store_true",
                   help="annotate nodes with runtime and peak memory")
    p.add_argument("--label-files", action="store_true",
                   help="label edges with the number of shared files")
    p.add_argument("--rankdir", default="TB", choices=["TB", "LR", "BT", "RL"],
                   help="graph direction (default: TB)")
    p.add_argument("--watch", metavar="DIR",
                   help="watch a directory and re-render its newest snapshot")
    p.add_argument("--interval", type=float, default=1.0,
                   help="polling interval in seconds for --watch (default: 1)")
    args = p.parse_args(argv)

    if args.watch:
        if args.format == "dot" and not args.output:
            args.output = os.path.join(args.watch, "physical-graph.dot")
        print(f"watching {args.watch} (Ctrl-C to stop)...", file=sys.stderr)
        last = None
        try:
            while True:
                path = newest_snapshot(args.watch)
                sig = (path, os.path.getmtime(path)) if path else None
                if path and sig != last:
                    last = sig
                    try:
                        render_one(path, args)
                    except (json.JSONDecodeError, OSError):
                        pass  # snapshot mid-write; retry next tick
                time.sleep(args.interval)
        except KeyboardInterrupt:
            return 0

    if not args.input:
        p.error("an input snapshot is required (or use --watch DIR)")
    render_one(args.input, args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
