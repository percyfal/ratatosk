import luigi
import os
import sys
import glob
from ratatosk.job import JobTask
from ratatosk.picard import MergeSamFiles, PicardMetrics
from ratatosk.fastq import FastqFileLink

class AlignSeqcap(JobTask):
    # Need to map sample to some tuple-like structure: {sample => {desired_output, bamfiles_to_merge}}
    sample = luigi.Parameter(default=[], is_list=True)
    flowcell = luigi.Parameter(default=[], is_list=True)
    label = luigi.Parameter(default=".sort.merge.bam", description="label used for final output")
    project = luigi.Parameter()
    indir = luigi.Parameter()

    def requires(self):
        fastq_list, bam_list = self.set_bam_files()
        for x in fastq_list:
            y = os.path.join(x[1], os.path.basename(x[0]))
            if not os.path.exists(y):
                if not os.path.exists(x[1]):
                    os.makedirs(x[1])
                os.symlink(os.path.relpath(x[0], os.path.dirname(y)), y)
        return [PicardMetrics(bam=b) for b in bam_list]

    def run(self):
        print "Analysing files {}".format(self.input())

    def set_bam_files(self):
        """Function for collecting samples and generating *target* file names"""
        project_indir = os.path.join(self.indir, self.project)
        if not os.path.exists(project_indir):
            return []
        fastq_list = []
        bam_list = []
        samples = os.listdir(project_indir)
        if self.sample:
            samples = self.sample
        for s in samples:
            print "Collecting data for sample {}".format(s)
            flowcells = os.listdir(os.path.join(project_indir, s))
            fl = []
            if self.flowcell:
                flowcells = self.flowcell
            for fc in flowcells:
                fastq_files = sorted(glob.glob(os.path.join(project_indir, s, fc, "{}*.fastq.gz".format(s))))
                fl.extend([(x, os.path.join(os.curdir, s, fc)) for x in fastq_files])
                fastq_list.extend([(x, os.path.join(os.curdir, s, fc)) for x in fastq_files])
            bam_list.append(os.path.join(fl[0][1], os.pardir, os.path.basename(fl[0][0]).replace(".fastq.gz", self.label).replace("_R1_001", "")))
        return fastq_list, bam_list

if __name__ == "__main__":
    luigi.run(main_task_cls=AlignSeqcap)
