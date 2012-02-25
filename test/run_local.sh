#! /bin/bash


rm -f test_results 

source ../bin/activate

export VERITABLE_KEY="test"
export VERITABLE_URL="http://127.0.0.1:5000"

export VERITABLE_NOSETEST_OPTIONS=""
echo -e "Running all tests\n" >> test_results
nosetests 2>> test_results
