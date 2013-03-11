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
	echo Running test 1: nosetests -v -s test_wrapper.py:TestLuigiWrappers.test_fastqln
	rm -f P* && nosetests -v -s test_wrapper.py:TestLuigiWrappers.test_fastqln
	;;
    2)  echo Running test 2: nosetests -v -s test_wrapper.py:TestLuigiWrappers.test_bwasampe
	rm -f P* && nosetests -v -s test_wrapper.py:TestLuigiWrappers.test_bwasampe
	;;
    3)  echo Running test 3: nosetests -v -s test_wrapper.py:TestLuigiWrappers.test_picard_metrics
	rm -f P* && nosetests -v -s test_wrapper.py:TestLuigiWrappers.test_picard_metrics
	;;
    *)
	echo "No such test" $ANALYSIS
	;;
esac
