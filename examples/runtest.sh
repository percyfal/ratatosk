#! /bin/bash
#
# File: runtest.sh
# Created: Mon Mar 11 11:42:50 2013
# $Id: $
#
# Copyright (C) 2013 by Per Unneberg
#
# Author: Per Unneberg
#
# Description:
# Use for demonstration purposes

if [ $# -ne 1 ]; then
    echo "ERROR: Wrong number of arguments"
    exit;
fi

ANALYSIS=$1

case $ANALYSIS in
    1)
	echo Running test 1: python pipeline.py  --project J.Doe_00_01 --indir ../../ngs_test_data/data/projects --config-file align_seqcap.yaml
	rm -rf P* && python pipeline.py  --project J.Doe_00_01 --indir ../../ngs_test_data/data/projects --config-file align_seqcap.yaml
	;;
    2)  echo Running test 2: python pipeline.py  --project J.Doe_00_01 --indir ../../ngs_test_data/data/projects --config-file align_adapter_trim_seqcap.yaml
	rm -rf P* && python pipeline.py  --project J.Doe_00_01 --indir ../../ngs_test_data/data/projects --config-file align_adapter_trim_seqcap.yaml
	;;
    3)  echo Running test 3: python pipeline_merge.py  --project J.Doe_00_01 --indir ../../ngs_test_data/data/projects --config-file align_seqcap_merge.yaml  --sample P001_101_index3
	rm -rf P* && python pipeline_merge.py  --project J.Doe_00_01 --indir ../../ngs_test_data/data/projects --config-file align_seqcap_merge.yaml  --sample P001_101_index3
	;;
    4)  echo Running test 4: python pipeline_custom.py  --project J.Doe_00_01 --indir ../../ngs_test_data/data/projects --config-file align_seqcap.yaml --sample P001_102_index6
	rm -rf P* && python pipeline_custom.py  --project J.Doe_00_01 --indir ../../ngs_test_data/data/projects --config-file align_seqcap.yaml --sample P001_102_index6
	;;
    *)
	echo "No such test" $ANALYSIS
	;;
esac
