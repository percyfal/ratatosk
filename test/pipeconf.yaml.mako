# section parameter values either affect the exe application or
# correspond to global application parameters
bwa:
  bwaref: ${bwaref}
  InputFastqFile:
    parent_task: ratatosk.lib.files.fastq.FastqFileLink

fastq:
  link:
    indir: ${fastq}

gatk:
  knownSites: 
    - ${knownSites1}
    - ${knownSites2}
  ref: ${ref}
  dbsnp: ${dbsnp}

picard:
  # InputBamFile "pipes" input from other modules
  InputBamFile:
    parent_task: ratatosk.lib.tools.samtools.SamToBam
  HsMetrics:
    parent_task: ratatosk.lib.tools.picard.SortSam
    targets: ${targets}
    baits: ${baits}
  DuplicationMetrics:
    parent_task: ratatosk.lib.tools.picard.SortSam
  AlignmentMetrics:
    parent_task: ratatosk.lib.tools.picard.SortSam
  InsertMetrics:
    parent_task: ratatosk.lib.tools.picard.SortSam

samtools:
  SamToBam:
    parent_task: test.test_wrapper.SampeToSamtools
