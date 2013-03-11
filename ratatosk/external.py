"""ratatosk.external

External tasks. The outputs of these tasks have
been created by some external process and therefore
have no run method.

"""
import os
import luigi
import logging

class BamFile(luigi.ExternalTask):
    bam = luigi.Parameter(default=None)

    def output(self):
        if not self.bam:
            return None
        return luigi.LocalTarget(os.path.abspath(self.bam))

class SamFile(luigi.ExternalTask):
    sam = luigi.Parameter(default=None)

    def output(self):
        if not self.sam:
            return None
        return luigi.LocalTarget(os.path.abspath(self.sam))
    
class FastqFile(luigi.ExternalTask):
    fastq = luigi.Parameter(default=None)

    def output(self):
        if not self.fastq:
            return None
        return luigi.LocalTarget(os.path.abspath(self.fastq))

class VcfFile(luigi.ExternalTask):
    vcf = luigi.Parameter(default=None)

    def output(self):
        if not self.vcf:
            return None
        return luigi.LocalTarget(os.path.abspath(self.vcf))
