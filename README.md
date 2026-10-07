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
ran. After each task it writes a JSON snapshot and a matching Graphviz `.dot`,
so the graph is available at every point during the run, not just at the end.

Each JSON snapshot follows a WfCommons-style layout (schemaVersion 1.5):

- `workflow.specification` holds the tasks (id, name, parents/children,
  input/output files) and the files with their sizes.
- `workflow.execution` holds per-task runtime, CPU, I/O and memory counters
  plus the machines the tasks ran on.

Parents and children come from the files: if task A writes a file that task B
reads, A is a parent of B.

## Get Started

Requires Nextflow 25.10 or newer.

In a pipeline you own, enable the plugin from its `nextflow.config`:

```groovy
plugins {
    id 'nf-bigbrother@1.0.1'
}

bigbrother {
    outputDir    = 'bigbrother'   // defaults shown
    emitPartials = true           // write a snapshot after every task
    emitDot      = true           // also write a .dot next to each json
    prefix       = ''
}
```

### Observing a Pipeline You Don't Own

For a pipeline you only run — an nf-core workflow, say — enable the plugin with
`NXF_PLUGINS_DEFAULT` rather than a `plugins` block:

```bash
NXF_PLUGINS_DEFAULT=nf-bigbrother@1.0.1 \
  nextflow run nf-core/demo -r 1.2.0 -profile test,docker --outdir results
```

Do **not** reach for `-plugins nf-bigbrother@1.0.1`, and do not put a `plugins`
block in a `-c` config. Both *replace* the pipeline's own `plugins` declaration
rather than adding to it, which silently unpins whatever the pipeline depends
on. nf-core/demo 1.2.0 pins `nf-schema@2.7.2`; unpinned, Nextflow 26.04 resolves
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

The `bigbrother` settings above work the same way in a `-c` config file; only
the `plugins` block has this replace-not-merge behaviour.

A run then fills `outputDir` with `partial_000_<name>_<uuid>.json` / `.dot`
(one per completed task), a final `complete_<name>_<uuid>.*`, and an `error_*`
snapshot instead if it dies partway.

The machine details and a few extra counters (`peak_rss`, `vol_ctxt`,
`inv_ctxt`) need an optional patch to Nextflow's task wrapper (see
[Plugin Development](#plugin-development)). Without the patch the plugin still
runs and produces the full graph; those specific fields are just left empty.

## Examples

Running any pipeline with the plugin enabled produces the snapshots described
above. Every snapshot already has a plain `.dot` beside it; the bundled Python
tools turn them into something nicer.

`tools/bb_dag.py` renders a snapshot (grouping by process, colour, node
metrics). It uses only the standard library, plus the `dot` binary for images:

```bash
tools/bb_dag.py bigbrother/complete_*.json -f svg --metrics -o graph.svg
tools/bb_dag.py --watch bigbrother -f svg -o live.svg   # re-render as a run goes
```

`tools/bb_animate.py` turns the whole sequence of snapshots into the kind of
animation shown at the top of this page:

```bash
tools/bb_animate.py bigbrother -o dag-build.gif --fps 14
```

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

Machine info and the extra counters are collected by a small patch to
Nextflow's task command wrapper, applied to your local Nextflow jar:

```bash
patch/patch-nextflow.sh              # patches $NXF_JAR (default: the 25.04.2 jar)
```

The patch injects a `bblog.log` into each task's work directory, which the
plugin reads back. It is tied to a specific Nextflow version's wrapper (the
bundled copy targets 25.04.2), so regenerate it if you patch a different
version. It is entirely optional; the plugin degrades gracefully without it.

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
