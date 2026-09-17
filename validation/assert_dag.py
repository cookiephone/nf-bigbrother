#!/usr/bin/env python3
"""Check the physical graph produced by validation/main.nf.

Run after `nextflow run validation/`: it reads the final snapshot and asserts
the known structure (SEED -> ALIGN x3, SAMPLE(sN) -> ALIGN(sN), ALIGN -> MERGE).
Exits non-zero with a message on the first failure.
"""
import glob
import json
import re
import sys


def short(name):
    return re.sub(r"\s*\(.*\)$", "", name).split(":")[-1]


def main(bb_dir):
    matches = sorted(glob.glob(f"{bb_dir}/complete_*.json"))
    if not matches:
        sys.exit(f"FAIL: no complete_*.json snapshot in {bb_dir}")
    tasks = {t["id"]: t for t in json.load(open(matches[-1]))["workflow"]["specification"]["tasks"]}
    by_proc = {}
    for t in tasks.values():
        by_proc.setdefault(short(t["name"]), []).append(t)

    def check(cond, msg):
        if not cond:
            sys.exit(f"FAIL: {msg}")

    # counts
    check(len(tasks) == 8, f"expected 8 tasks, got {len(tasks)}")
    for proc, n in (("SEED", 1), ("SAMPLE", 3), ("ALIGN", 3), ("MERGE", 1)):
        check(len(by_proc.get(proc, [])) == n,
              f"expected {n} {proc} task(s), got {len(by_proc.get(proc, []))}")

    # structure: this only holds if globbed outputs (*.reads, *.bam) are linked
    seed = by_proc["SEED"][0]
    merge = by_proc["MERGE"][0]
    check(len(seed["children"]) == 3, f"SEED should feed 3 ALIGN tasks, has {len(seed['children'])}")
    check(len(merge["parents"]) == 3, f"MERGE should consume 3 ALIGN tasks, has {len(merge['parents'])}")
    for a in by_proc["ALIGN"]:
        check(len(a["parents"]) == 2, f"each ALIGN should have 2 parents (SEED + SAMPLE), {a['id']} has {len(a['parents'])}")
        check(len(a["children"]) == 1, f"each ALIGN should feed MERGE, {a['id']} has {len(a['children'])} children")

    # generic integrity: symmetric, no dangling
    ids = set(tasks)
    for i, t in tasks.items():
        for c in t["children"]:
            check(c in ids and i in tasks[c]["parents"], f"asymmetric/dangling edge {i}->{c}")

    print(f"OK: {len(tasks)} tasks, physical DAG correct (SEED->ALIGNx3, SAMPLE->ALIGN, ALIGN->MERGE)")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "bb_out")
