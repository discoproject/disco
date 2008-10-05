#!/bin/bash

export PYTHONPATH=../pydisco

if [[ -z $1 ]]
then
        echo "Usage:"
        echo "./run_tests.sh disco://master [test_name]"
        exit
fi

for tst in test_*.py
do
        if [[ ! -z $2 && $tst != $2 ]]
        then
                continue
        fi
        echo "Running $tst"
        python $tst $1 2> $tst.err.log > $tst.log
        if [[ `tail -1 $tst.log` == "ok" ]]
        then
                echo "Test $tst ok."
        else
                echo "Test $tst failed. See $tst.*.log"
                break
        fi
done
