// Self-contained pipeline used to validate the observer end to end.
// It has a shared reference, a fan-out over three samples with glob outputs,
// and a fan-in, so the physical graph it produces is known and can be checked
// (see assert_dag.py). No containers or external data required.

process SEED {
    output:
    path 'ref.txt'
    script:
    'echo reference > ref.txt'
}

process SAMPLE {
    tag "$name"
    input:
    val name
    output:
    tuple val(name), path("*.reads")
    script:
    "echo $name > ${name}.reads"
}

process ALIGN {
    tag "$name"
    input:
    tuple val(name), path(reads)
    path ref
    output:
    path "*.bam"
    script:
    "cat $reads $ref > ${name}.bam"
}

process MERGE {
    input:
    path bams
    output:
    path 'merged.txt'
    script:
    'cat *.bam > merged.txt'
}

workflow {
    ref   = SEED()
    reads = SAMPLE(Channel.of('s1', 's2', 's3'))
    bams  = ALIGN(reads, ref)
    MERGE(bams.collect())
}
