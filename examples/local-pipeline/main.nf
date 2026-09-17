#!/usr/bin/env nextflow
nextflow.enable.dsl = 2

/*
 * A container-free stand-in for a real workflow, used to check the observer.
 * It has the parts that make a physical graph interesting: a reference built
 * once and shared, a fan-out over samples, glob and directory outputs, and a
 * fan-in at the end. See README.md for the graph it should produce.
 */

// PREPARE_GENOME: fixed-name output, one instance, feeds every ALIGN task
process PREPARE_GENOME {
    input:
    path raw
    output:
    path 'genome.idx'
    script:
    """
    cat ${raw} > genome.idx
    echo "index-built" >> genome.idx
    """
}

// TRIM: glob output *.trimmed.fastq, one instance per sample
process TRIM {
    tag "$sample"
    input:
    tuple val(sample), path(reads)
    output:
    tuple val(sample), path("*.trimmed.fastq")
    script:
    """
    cat ${reads} > ${sample}.trimmed.fastq
    """
}

// ALIGN: consumes trimmed reads + the shared genome index, glob output *.bam
process ALIGN {
    tag "$sample"
    input:
    tuple val(sample), path(trimmed)
    path index
    output:
    tuple val(sample), path("*.bam")
    script:
    """
    cat ${trimmed} ${index} > ${sample}.bam
    """
}

// INDEX_BAM: consumes the bam, produces a directory of outputs (salmon-style)
process INDEX_BAM {
    tag "$sample"
    input:
    tuple val(sample), path(bam)
    output:
    tuple val(sample), path("${sample}_idx")
    script:
    """
    mkdir ${sample}_idx
    cp ${bam} ${sample}_idx/aligned.bam
    echo "stats" > ${sample}_idx/stats.txt
    echo "log"   > ${sample}_idx/run.log
    """
}

// MULTIQC: fan-in, consumes every sample's index directory
process MULTIQC {
    input:
    path 'idx/*'
    output:
    path 'report.html'
    script:
    """
    echo "<html>report over: \$(ls idx)</html>" > report.html
    """
}

workflow {
    // three synthetic samples
    samples = Channel.of(
        ['sampleA', "${workflow.projectDir}/data/sampleA.fastq"],
        ['sampleB', "${workflow.projectDir}/data/sampleB.fastq"],
        ['sampleC', "${workflow.projectDir}/data/sampleC.fastq"],
    ).map { name, f -> tuple(name, file(f)) }

    genome = PREPARE_GENOME(file("${workflow.projectDir}/data/genome.fasta"))

    trimmed = TRIM(samples)
    aligned = ALIGN(trimmed, genome)
    indexed = INDEX_BAM(aligned)

    MULTIQC(indexed.map { sample, dir -> dir }.collect())
}
