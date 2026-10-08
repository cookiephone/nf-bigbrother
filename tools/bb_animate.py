#!/usr/bin/env python3
"""Turn a run's event log into a GIF of the graph filling in.

Replays the run a task at a time and builds one animated GIF where the physical
graph grows as it goes. The layout is done once on the final graph and then
pinned, so nodes don't jump around between frames — each frame just shows the
tasks that had run by that point, with the new ones highlighted.

Reads `events_*.jsonl` by preference, falling back to partial_*/complete_*
snapshots for runs recorded before the event log existed.

    bb_animate.py bb_out -o dag-build.gif --fps 12
"""
from __future__ import annotations

import argparse
import functools
import glob
import os
import re
import subprocess
import sys
import tempfile

from PIL import Image, ImageDraw, ImageFont

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bb_dag  # noqa: E402  (reuse Snapshot, short_name, sample_tag, PALETTE)

GHOST_FILL = "white"
GHOST_LINE = "#e2e2e2"
NEW_LINE = "#d00000"
EDGE_COLOR = "#888888"
NEW_EDGE = "#d00000"


def snapshot_files(directory: str) -> list[str]:
    """Snapshots in run order: partial_000, partial_001, …, then complete."""
    partials = sorted(glob.glob(os.path.join(directory, "partial_*.json")),
                      key=lambda p: int(re.search(r"partial_(\d+)", p).group(1)))
    final = sorted(glob.glob(os.path.join(directory, "complete_*.json"))) or \
        sorted(glob.glob(os.path.join(directory, "error_*.json")))
    return partials + final


def frame_loaders(directory: str) -> list:
    """One callable per frame, in run order, each returning that frame's graph.

    Prefers the event log: it holds a line per task, so the state after task N
    is just a replay of the first N lines -- the same sequence the partial_*
    snapshots used to provide, without the run paying to rewrite the whole
    graph after every task. Falls back to partial_* files when a run was
    recorded with `emitPartials` and no event log.
    """
    logs = glob.glob(os.path.join(directory, "events_*.jsonl"))
    if logs:
        path = max(logs, key=os.path.getmtime)
        total = bb_dag.count_task_events(path)
        if total:
            return [functools.partial(bb_dag.Snapshot.from_events, path, n)
                    for n in range(1, total + 1)]
    return [functools.partial(bb_dag.Snapshot.load, f) for f in snapshot_files(directory)]


def node_label(snap: bb_dag.Snapshot, tid: str) -> str:
    task = snap.tasks[tid]
    parts = [bb_dag.short_name(task.get("name", ""))]
    tag = bb_dag.sample_tag(task.get("name", ""))
    if tag:
        parts.append(tag)
    return "\\n".join(p.replace("\\", "\\\\").replace('"', '\\"') for p in parts)


def compute_layout(final: bb_dag.Snapshot, rankdir: str, nodesep: float,
                   ranksep: float) -> tuple[dict, float, float]:
    """Run `dot` on the final graph and return {tid: (x, y, w, h)} in points."""
    lines = ["digraph g {", f"  rankdir={rankdir};",
             f"  nodesep={nodesep}; ranksep={ranksep};",
             '  node [shape=box, fixedsize=true, fontname="Helvetica", fontsize=11];']
    for tid in final.tasks:
        lines.append(f'  "t{tid}" [label="{node_label(final, tid)}"];')
    for parent, child in final.edges(False):
        lines.append(f'  "t{parent}" -> "t{child}";')
    lines.append("}")
    plain = subprocess.run(["dot", "-Tplain"], input="\n".join(lines).encode(),
                           capture_output=True, check=True).stdout.decode()

    pos: dict[str, tuple[float, float, float, float]] = {}
    gw = gh = 0.0
    for line in plain.splitlines():
        tok = line.split()
        if tok[0] == "graph":
            gw, gh = float(tok[2]) * 72, float(tok[3]) * 72
        elif tok[0] == "node":
            name, x, y, w, h = tok[1].strip('"'), *map(float, tok[2:6])
            pos[name.lstrip("t")] = (x * 72, y * 72, w, h)
    return pos, gw, gh


def frame_dot(final: bb_dag.Snapshot, pos: dict, present: set, edges: set,
              new_nodes: set, new_edges: set, colour_of: dict) -> str:
    out = ["digraph g {",
           '  node [shape=box, style="rounded,filled", fixedsize=true, '
           'fontname="Helvetica", fontsize=11, penwidth=1];']
    for tid, (x, y, w, h) in pos.items():
        common = f'pos="{x},{y}", width={w:.3f}, height={h:.3f}'
        if tid in present:
            proc = bb_dag.short_name(final.tasks[tid].get("name", ""))
            fill = colour_of.setdefault(proc, bb_dag.PALETTE[len(colour_of) % len(bb_dag.PALETTE)])
            line, pw = (NEW_LINE, 3.5) if tid in new_nodes else ("#333333", 1)
            out.append(f'  "t{tid}" [{common}, label="{node_label(final, tid)}", '
                       f'fillcolor="{fill}", color="{line}", penwidth={pw}];')
        else:  # ghost slot for a task that has not run yet (keeps the canvas fixed)
            out.append(f'  "t{tid}" [{common}, label="", fillcolor="{GHOST_FILL}", '
                       f'color="{GHOST_LINE}"];')
    for parent, child in edges:
        colour, pw = (NEW_EDGE, 2.5) if (parent, child) in new_edges else (EDGE_COLOR, 1)
        out.append(f'  "t{parent}" -> "t{child}" [color="{colour}", penwidth={pw}, arrowsize=0.6];')
    out.append("}")
    return "\n".join(out)


def render_png(dot_text: str, path: str) -> None:
    # -n2 : use the supplied node positions verbatim, do not re-layout
    subprocess.run(["neato", "-n2", "-Tpng", "-o", path],
                   input=dot_text.encode(), capture_output=True, check=True)


def load_font(size: int):
    for name in ("DejaVuSans-Bold.ttf", "DejaVuSans.ttf"):
        try:
            return ImageFont.truetype(name, size)
        except OSError:
            continue
    return ImageFont.load_default()


def caption(img: Image.Image, text: str, total: int, done: int, bar_h: int) -> Image.Image:
    """Add a caption strip with a progress bar above the frame."""
    w = img.width
    canvas = Image.new("RGB", (w, img.height + bar_h), "white")
    canvas.paste(img, (0, bar_h))
    draw = ImageDraw.Draw(canvas)
    font = load_font(max(14, bar_h // 3))
    draw.text((14, bar_h // 2), text, fill="#222222", font=font, anchor="lm")
    # progress bar bottom of the strip
    pad, y = 14, bar_h - 8
    frac = done / total if total else 0
    draw.rectangle([pad, y, w - pad, y + 4], fill="#e5e5e5")
    draw.rectangle([pad, y, pad + int((w - 2 * pad) * frac), y + 4], fill="#d00000")
    return canvas


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("snapshot_dir",
                   help="a run's output directory (events_*.jsonl, or partial_*/complete_* snapshots)")
    p.add_argument("-o", "--output", default="dag-build.gif", help="output GIF path")
    p.add_argument("--fps", type=float, default=12, help="frames per second (default 12)")
    p.add_argument("--every", type=int, default=1, help="keep every Nth snapshot (default 1)")
    p.add_argument("--max-width", type=int, default=2400, help="downscale frames wider than this")
    p.add_argument("--hold", type=float, default=2.5, help="seconds to hold the final frame")
    p.add_argument("--rankdir", default="TB", choices=["TB", "LR"], help="layout direction")
    p.add_argument("--nodesep", type=float, default=0.2, help="dot nodesep (inches)")
    p.add_argument("--ranksep", type=float, default=1.0, help="dot ranksep (inches)")
    args = p.parse_args(argv)

    loaders = frame_loaders(args.snapshot_dir)
    if not loaders:
        sys.exit(f"no event log or snapshots found in {args.snapshot_dir}")
    # always keep the final frame even when sampling
    kept = loaders[::args.every]
    if loaders[-1] is not kept[-1]:
        kept.append(loaders[-1])

    final = loaders[-1]()
    print(f"laying out final graph ({len(final.tasks)} tasks) with dot...", file=sys.stderr)
    pos, _, _ = compute_layout(final, args.rankdir, args.nodesep, args.ranksep)

    colour_of: dict[str, str] = {}
    frames: list[Image.Image] = []
    prev_present: set = set()
    prev_edges: set = set()
    scale = None
    bar_h = 64

    with tempfile.TemporaryDirectory() as tmp:
        for i, load in enumerate(kept):
            snap = load()
            present = {t for t in snap.tasks if t in pos}
            edges = {(a, b) for a, b in snap.edges(False) if a in pos and b in pos}
            new_nodes = present - prev_present
            new_edges = edges - prev_edges

            dot_text = frame_dot(final, pos, present, edges, new_nodes, new_edges, colour_of)
            png = os.path.join(tmp, f"f{i:04d}.png")
            render_png(dot_text, png)

            img = Image.open(png).convert("RGB")
            if scale is None:
                scale = min(1.0, args.max_width / img.width)
            if scale < 1.0:
                img = img.resize((round(img.width * scale), round(img.height * scale)))
            text = f"nf-bigbrother physical execution graph   |   {len(present)}/{len(final.tasks)} tasks"
            frames.append(caption(img, text, len(final.tasks), len(present), bar_h))

            prev_present, prev_edges = present, edges
            if (i + 1) % 20 == 0 or i + 1 == len(kept):
                print(f"  rendered {i + 1}/{len(kept)} frames", file=sys.stderr)

    per_frame_ms = int(1000 / args.fps)
    durations = [per_frame_ms] * len(frames)
    durations[-1] = int(args.hold * 1000)
    frames[0].save(args.output, save_all=True, append_images=frames[1:],
                   duration=durations, loop=0, optimize=True)
    size_mb = os.path.getsize(args.output) / 1e6
    print(f"wrote {args.output}  ({len(frames)} frames, {size_mb:.1f} MB)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
