import luigi
import os
import glob
from ratatosk.job import JobTask
from ratatosk.picard import PicardMetrics, SortSam
from ratatosk.fastq import FastqFileLink

class AlignSeqcap(luigi.WrapperTask):
    sample = luigi.Parameter(default=[], is_list=True)
    flowcell = luigi.Parameter(default=[], is_list=True)
    label = luigi.Parameter(default=".sort.bam", description="label used for final output")
    project = luigi.Parameter()
    indir = luigi.Parameter()

    # NB: ok, problem: we need to know what the target names will look
    # like. would like to work with more generic feature, e.g. sample
    # name
    def requires(self):
        fastq_list, bam_list = self.set_bam_files()
        # Hack: since the scheduler is asynchronous, we need to make
        # sure the fastq file links exist. The following construct
        # will not work:
        #
        # return [FastqFileLink(fastq=x[0], outdir=x[1]) for x in fastq_list] + [PicardMetrics(bam=y) for y in bam_list]
        for x in fastq_list:
            y = os.path.join(x[1], os.path.basename(x[0]))
            if not os.path.exists(y):
                if not os.path.exists(x[1]):
                    os.makedirs(x[1])
                os.symlink(os.path.relpath(x[0], os.path.dirname(y)), y)
        return [PicardMetrics(bam=y) for y in bam_list]

    def run(self):
        print "Analysing files {}".format(self.input())

    def set_bam_files(self):
        """Function for collecting samples and generating *target* file names"""
        project_indir = os.path.join(self.indir, self.project)
        if not os.path.exists(project_indir):
            return []
        fastq_list = []
        samples = os.listdir(project_indir)
        if self.sample:
            samples = self.sample
        for s in samples:
            flowcells = os.listdir(os.path.join(project_indir, s))
            if self.flowcell:
                flowcells = self.flowcell
            for fc in flowcells:
                # NB: 'sorted' makes all the difference between MacOSX and Linux...
                fastq_files = sorted(glob.glob(os.path.join(project_indir, s, fc, "{}*.fastq.gz".format(s))))
                fastq_list.extend([(x, os.path.join(os.curdir, s, fc)) for x in fastq_files])
        bam_list = []
        for i in range(0, len(fastq_list), 2):
            bam_list.append(os.path.join(fastq_list[i][1], os.path.basename(fastq_list[i][0]).replace(".fastq.gz", self.label).replace("_R1_001", "")))
        return fastq_list, bam_list

if __name__ == "__main__":
    luigi.run(main_task_cls=AlignSeqcap)
