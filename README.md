# nf-bigbrother

A Nextflow trace observer that records the *physical* execution graph of a run
(one node per task instance, with an edge wherever one task's output file is
another task's input) and writes it out incrementally, so you can watch the
graph being built while the pipeline is still running.

![nf-core/rnaseq physical execution graph being built up as the run proceeds](docs/rnaseq-dag.gif)

*nf-core/rnaseq (test profile): 209 tasks filling in one at a time, coloured by
process, with each snapshot's newest tasks and edges highlighted.*

## Summary

Nextflow's own `-with-dag` gives you the abstract process/channel graph that's
known before anything runs. `nf-bigbrother` records the graph of what actually
ran. It appends one JSON line per task to an event log as the run proceeds, and
writes a full JSON snapshot plus a matching Graphviz `.dot` at the end, so the
graph is available at every point during the run and not just when it finishes.

The final snapshot is a valid WfCommons WfFormat instance (schemaVersion 1.6):

- `workflow.specification` holds the tasks (id, name, parents/children,
  input/output files) and the files with their sizes.
- `workflow.execution` holds per-task runtime, CPU, I/O and memory counters,
  what each task requested of the scheduler, and the machines they ran on.

Parents and children come from the files: if task A writes a file that task B
reads, A is a parent of B.

The event log (`events_*.jsonl`) carries the same information incrementally:
a `run` header, one `task` line each time a task finishes, and a terminal
`complete` (or `error`) line with the file table and makespan. Replaying its
first *N* task lines reconstructs the graph as it stood after *N* tasks, which
is what the bundled tools use to render a run as it goes. It is the cheap path:
a full snapshot re-serialises the entire graph, so writing one per task costs
time and space quadratic in the task count, while the log costs one line per
task. That is why snapshots-during-the-run are off by default.

## Get Started

Requires Nextflow 25.10 or newer.

In a pipeline you own, enable the plugin from its `nextflow.config`:

```groovy
plugins {
    id 'nf-bigbrother@1.0.1'
}

bigbrother {
    outputDir    = 'bigbrother'   // defaults shown
    emitEvents   = true           // append a JSON line per task to events_*.jsonl
    emitPartials = false          // also write a whole-graph snapshot during the run
    snapshotEvery = 1             // ...every N tasks, when emitPartials is on
    emitDot      = true           // write a .dot beside every json snapshot
    prefix       = ''
}
```

`emitPartials` defaulted to `true` up to 1.0.1, which wrote a
`partial_NNN_*.json` after every task. The event log supersedes it, carrying
the same information one line per task instead of a full rewrite, so it now
defaults to `false`. Turn it back on if you have tooling that reads the
partial files, and use `snapshotEvery` to thin them out on a large run.

### Observing a Pipeline You Don't Own

For a pipeline you only run, an nf-core workflow say, enable the plugin with
`NXF_PLUGINS_DEFAULT` rather than a `plugins` block:

```bash
NXF_PLUGINS_DEFAULT=nf-bigbrother@1.0.1 \
  nextflow run nf-core/demo -r 1.2.0 -profile test,docker --outdir results
```

Do **not** reach for `-plugins nf-bigbrother@1.0.1`, and do not put a `plugins`
block in a `-c` config. Both *replace* the pipeline's own `plugins` declaration
rather than adding to it, which silently unpins whatever the pipeline depends
on. nf-core/demo 1.2.0 pins `nf-schema@2.7.2`. Unpinned, Nextflow 26.04 resolves
it to `nf-schema@3.0.0`, whose changed `paramsSummaryLog` signature aborts the
run before a single task starts:

```
Missing process or function paramsSummaryLog(...)
```

`NXF_PLUGINS_DEFAULT` feeds a separate list that is merged with the pipeline's,
so the pins survive. If you do need the plugin declared in config, restate the
pipeline's own plugins alongside it in a single block:

```groovy
plugins {
    id 'nf-schema@2.7.2'      // whatever the pipeline already pinned
    id 'nf-bigbrother@1.0.1'
}
```

The `bigbrother` settings above work the same way in a `-c` config file. Only
the `plugins` block has this replace-not-merge behaviour.

A run then fills `outputDir` with `events_<name>_<uuid>.jsonl` (appended to as
it goes), a final `complete_<name>_<uuid>.json` / `.dot`, and an `error_*`
snapshot instead if it dies partway. With `emitPartials` on you also get
`partial_NNN_<name>_<uuid>.json` / `.dot`.

The `machines` list, the hardware description of the nodes tasks ran on,
needs an optional patch to Nextflow's task wrapper (see
[Plugin Development](#plugin-development)). Everything else, including
`peak_rss`, `peak_vmem`, `vol_ctxt` and `inv_ctxt`, comes from stock Nextflow
and needs no patch. Note that those counters are *sampled*, first at 1 s and
then less often, so a task that finishes in milliseconds reports zero for them
however it was launched.

### What Each Task Records

Alongside the WfCommons fields, each task execution carries a `bigbrother`
object with what the task asked the scheduler for and how it fared:

| group | fields |
| --- | --- |
| `requested` | `cpus`, `memoryInBytes`, `diskInBytes`, `timeInMillis` |
| `timing` | `pendingAt`, `submittedAt`, `completedAt`, `queueWaitSeconds`, `durationSeconds` |
| `outcome` | `attempt`, `exitStatus`, `status`, `errorAction` |
| `placement` | `queue`, `executor`, `container`, `cpuModel`, `hostname`, `nativeId` |
| `identity` | `process`, `tag`, `hash` |

The requested values are what make the measured ones interpretable: a task that
peaked at 3 GB is unremarkable until you know it reserved 72 GB. `queueWaitSeconds`
is the gap between the executor submitting the task and the task starting, and
is `null` rather than `0` when either timestamp is missing, so a genuine
zero wait stays distinguishable from an unknown one. `pendingAt` is when
Nextflow itself queued the task, which is earlier than `submittedAt` and lets
you separate waiting inside Nextflow from waiting inside the scheduler.

`hostname` is reported by Nextflow but populated by none of the built-in
executors, so it is normally empty. The patch's machine details are what
identify a node today.

## Examples

Running any pipeline with the plugin enabled produces the output described
above. The final snapshot already has a plain `.dot` beside it, and the
bundled Python tools turn either that or the event log into something nicer.

`tools/bb_dag.py` renders a graph (grouping by process, colour, node metrics).
It uses only the standard library, plus the `dot` binary for images:

```bash
tools/bb_dag.py bigbrother/complete_*.json -f svg --metrics -o graph.svg
tools/bb_dag.py bigbrother/events_*.jsonl -f svg -o graph.svg   # same graph
tools/bb_dag.py --watch bigbrother -f svg -o live.svg   # re-render as a run goes
```

`tools/bb_animate.py` replays a run into the kind of animation shown at the top
of this page, one frame per task:

```bash
tools/bb_animate.py bigbrother -o dag-build.gif --fps 14
```

It reads the event log by preference and falls back to `partial_*` snapshots,
so runs recorded either way animate the same.

There is a small container-free pipeline under `examples/local-pipeline` (a
shared reference, a fan-out over samples, glob and directory outputs, a fan-in)
that produces a correct graph in a few seconds:

```bash
cd examples/local-pipeline
nextflow run main.nf
../../tools/bb_dag.py bb_out/complete_*.json -f png --metrics -o graph.png
```

The workflow used as the real reference is nf-core/rnaseq on the test profile:

```bash
NXF_PLUGINS_DEFAULT=nf-bigbrother@1.0.1 \
  nextflow run nf-core/rnaseq -r 3.21.0 -profile test,docker --outdir results
```

## Plugin Development

Build, test, install and publish with the Makefile:

```bash
make assemble    # build the plugin zip
make test        # unit tests
make install     # install into ~/.nextflow/plugins
make e2e         # run the validation pipeline and check the graph it produces
make release     # publish to the Nextflow registry
```

`validation/` holds the end-to-end pipeline and `assert_dag.py`, which checks
the produced physical graph against a known structure. CI (`.github/workflows`)
runs the unit tests and the e2e test across Java 17/21 and Nextflow 25.10/26.04.

### The Command-Wrapper Patch

Machine info is collected by a small patch to Nextflow's task command wrapper,
applied to your local Nextflow jar:

```bash
patch/patch-nextflow.sh              # patches $NXF_JAR (default: the 25.04.2 jar)
```

The patch injects a `bblog.log` into each task's work directory, which the
plugin reads back for the node name and its hardware. That is *all* it adds:
diffing `patch/custom-command-trace.txt` against the stock wrapper shows the
`bblog.log` block and nothing else of substance, so the resource counters are
stock Nextflow either way. It is tied to a specific Nextflow version's wrapper
(the bundled copy targets 25.04.2), so regenerate it if you patch a different
version. It is entirely optional, and the plugin degrades gracefully without
it.

Because it needs a patched jar, the patch is also the one thing that makes the
plugin awkward to deploy somewhere you do not administer. Everything needed to
compare requested against used resources works unpatched, from a single
`plugins` line.

## License

Apache License 2.0. See the [`COPYING`](COPYING) file.

If you are publishing a paper and use this plugin, please consider citing our publication:

```bibtex
@article{kharma2026comprehensive,
  title={Comprehensive Plugin-Based Monitoring of Nexflow Workflow Executions},
  author={Kharma, Sami and Wies, Tobias and Schintke, Florian},
  journal={arXiv preprint arXiv:2603.28783},
  year={2026}
}
```
