# section parameter values either affect the exe application or
# correspond to global application parameters
cutadapt:
  parent_task: ratatosk.lib.files.fastq.FastqFileLink

misc:
  ResyncMates:
    parent_task: ratatosk.lib.utils.cutadapt.CutadaptJobTask

bwa:
  bwaref: ${bwaref}

fastq:
  link:
    indir: ${fastq}

gatk:
  knownSites: 
    - ${knownSites1}
    - ${knownSites2}
  ref: ${ref}
  dbsnp: ${dbsnp}
  RealignerTargetCreator:
    parent_task: ratatosk.lib.tools.picard.MergeSamFiles
  IndelRealigner:
    parent_task: ratatosk.lib.tools.picard.MergeSamFiles
  BaseRecalibrator:
    parent_task: ratatosk.lib.tools.gatk.IndelRealigner
  PrintReads:
    parent_task: ratatosk.lib.tools.gatk.BaseRecalibrator
  ClipReads:
    parent_task: ratatosk.lib.tools.gatk.PrintReads
  UnifiedGenotyper:
    parent_task: ratatosk.lib.tools.gatk.ClipReads
  VariantFiltration:
    parent_task: ratatosk.lib.tools.gatk.UnifiedGenotyper
  VariantEval:
    parent_task: ratatosk.lib.tools.gatk.VariantFiltration

picard:
  InputBamFile:
    parent_task: ratatosk.lib.tools.samtools.SamToBam
  SortSam:
    parent_task: ratatosk.lib.tools.samtools.SamToBam
  HsMetrics:
    parent_task: ratatosk.lib.tools.picard.SortSam
    target_regions: ${targets}
    bait_regions: ${baits}
  DuplicationMetrics:
    parent_task: ratatosk.lib.tools.picard.SortSam
  AlignmentMetrics:
    parent_task: ratatosk.lib.tools.picard.SortSam
  InsertMetrics:
    parent_task: ratatosk.lib.tools.picard.SortSam
  MergeSamFiles:
    parent_task: ratatosk.lib.tools.picard.SortSam
    target_generator_function: test.site_functions.organize_sample_runs

samtools:
  SamToBam:
    parent_task: ratatosk.lib.align.bwa.BwaSampe

