#! /bin/bash


rm -f test_results 

export VERITABLE_NOSETEST_OPTIONS="nogzip:nossl"
echo -e "Running all tests with gzip and ssl disabled\n" >> test_results
nosetests 2>> test_results

export VERITABLE_NOSETEST_OPTIONS="nogzip"
echo -e "Running all tests with gzip disabled\n" >> test_results
nosetests 2>> test_results

export VERITABLE_NOSETEST_OPTIONS="nossl"
echo -e "Running all tests with ssl disabled\n" >> test_results
nosetests 2>> test_results

export VERITABLE_NOSETEST_OPTIONS=""
echo -e "Running all tests\n" >> test_results
nosetests 2>> test_results
