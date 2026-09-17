# nf-bigbrother

A Nextflow trace observer that records the *physical* execution graph of a run
(one node per task instance, with an edge wherever one task's output file is
another task's input) and writes it out incrementally, so you can watch the
graph being built while the pipeline is still running.

![nf-core/rnaseq physical execution graph being built up as the run proceeds](docs/rnaseq-dag.gif)

*nf-core/rnaseq (test profile): 209 tasks filling in one at a time, coloured by
process, with each snapshot's newest tasks and edges highlighted.*

Nextflow's own `-with-dag` gives you the abstract process/channel graph that's
known before anything runs. This is the graph of what actually ran. After each
task the plugin writes a JSON snapshot and a matching Graphviz `.dot`. The JSON
follows a WfCommons-style layout (schemaVersion 1.5): `workflow.specification`
holds the tasks (id, name, parents/children, input/output files) and the files
with their sizes; `workflow.execution` holds per-task runtime, CPU, I/O and
memory counters plus the machines things ran on. Parents and children come from
the files: if task A writes a file that task B reads, A is a parent of B.

## Install and Enable

Needs Nextflow >= 25.04.2 and a JDK 17+.

```bash
make install    # builds the plugin zip and drops it in ~/.nextflow/plugins
```

In your `nextflow.config`:

```groovy
plugins { id 'nf-bigbrother@0.7.0' }

bigbrother {
  outputDir    = 'bigbrother'   // defaults shown
  emitPartials = true           // write a snapshot after every task
  emitDot      = true           // also write a .dot next to each json
  prefix       = ''
}
```

A run then fills `outputDir` with `partial_000_<name>_<uuid>.json` /
`.dot` (one per completed task), a final `complete_<name>_<uuid>.*`, and an
`error_*` snapshot instead if it dies partway.

### The Command-Wrapper Patch

Machine info (`bblog.log`) and the extra counters (`peak_rss`, `vol_ctxt`,
`inv_ctxt`) come from a small patch to Nextflow's task wrapper. Apply it once:

```bash
./tools.sh nf-setup     # installs Nextflow 25.04.2 and patches it
# or against an existing install:
patch/patch-nextflow.sh
```

Without the patch the plugin still works. You just get empty machine specs and
zeros for the patched-only counters; the graph, file sizes and the standard
metrics are unaffected.

## Looking at the Graph

Every snapshot already has a plain `.dot` beside it. `tools/bb_dag.py` is a
nicer renderer (grouping by process, colour, node metrics, live-follow). It only
uses the standard library, but needs the `dot` binary to produce images:

```bash
tools/bb_dag.py bb_out/complete_*.json                     # dot to stdout
tools/bb_dag.py bb_out/complete_*.json -f svg --metrics     # svg with runtime/mem
tools/bb_dag.py bb_out/partial_007_*.json --completed-only --rankdir LR -f png
tools/bb_dag.py --watch bb_out -f svg -o live.svg           # re-render as a run goes
```

Other flags: `--no-cluster`, `--label-files`, `-f {dot,svg,png,pdf}`.

`tools/bb_animate.py` stitches the whole sequence of snapshots into a GIF of the
graph filling in as the run proceeds (the animation above). It lays the final
graph out once and pins the positions, so nodes stay put and each frame just
reveals the tasks that had run by then. Needs Graphviz and Pillow:

```bash
tools/bb_animate.py bb_out -o dag-build.gif --fps 14
```

## Trying It

`examples/local-pipeline` is a container-free pipeline built out of `cat` and
`mkdir` that still has the interesting bits (a shared reference, a fan-out over
samples, glob and directory outputs, a fan-in), so you can get a correct graph
in a few seconds:

```bash
cd examples/local-pipeline
nextflow run main.nf
../../tools/bb_dag.py bb_out/complete_*.json -f png --metrics -o graph.png
```

The workflow I use as the real reference is nf-core/rnaseq on the test profile:

```bash
nextflow run nf-core/rnaseq -r 3.21.0 -profile test,docker --outdir results
```

## Layout

```
plugins/nf-bigbrother/src/main/nextflow/bigbrother/
  BigBrotherObserver.groovy   the observer: handles events, writes snapshots
  WfInstance.groovy           the model + json/dot rendering
  BigBrotherConfig.groovy     the bigbrother{} config block
  BigBrotherFactory.groovy    registers the observer
  BigBrotherPlugin.groovy     plugin entry point
tools/bb_dag.py, bb_animate.py   render / animate a snapshot
patch/                        the command-wrapper patch
examples/local-pipeline/      demo pipeline
```

`make compile` / `make install` / `make check` for the usual build steps.
