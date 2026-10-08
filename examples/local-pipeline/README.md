# Local example pipeline

A tiny pipeline for exercising the plugin without containers. It's just `cat`
and `mkdir`, but it keeps the parts that make a physical graph worth looking at:
a reference (`PREPARE_GENOME`) built once and used by every `ALIGN`, a per-sample
`TRIM → ALIGN → INDEX_BAM` chain over three samples, glob outputs
(`*.trimmed.fastq`, `*.bam`) and a directory output (`*_idx`), and a `MULTIQC`
fan-in at the end. The dependencies are known, so the graph the plugin infers
can be checked against what you'd draw by hand.

```bash
nextflow run main.nf
../../tools/bb_dag.py bb_out/complete_*.json -f png --metrics --label-files -o graph.png
```

That should give you:

```
PREPARE_GENOME ─┬─► ALIGN(A) ─► INDEX_BAM(A) ─┐
   TRIM(A) ─────┘                             │
   TRIM(B) ─────► ALIGN(B) ─► INDEX_BAM(B) ───┼─► MULTIQC
   TRIM(C) ─────► ALIGN(C) ─► INDEX_BAM(C) ───┘
```
