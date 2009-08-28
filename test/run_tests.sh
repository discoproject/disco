#!/bin/bash

bin=`dirname $0`
cd $bin

export PYTHONPATH=../pydisco
TESTS=(test_*.py)
NUM_TESTS=${#TESTS[*]}

if [[ -z $1 ]]
then
        echo -e "\nUsage:"
        echo -e "\n./run_tests.sh disco://master [test id]"
        echo -e "\nTests:\n"
        for ((i=0; i<$NUM_TESTS; i++));
        do
                echo "$i) ${TESTS[$i]}"
        done
        echo
        exit
fi

if [[ $2 ]]
then
	FIRST=$2
else
	FIRST=0
fi

for ((i=$FIRST; i<$NUM_TESTS; i++));
do
	tst=${TESTS[$i]}
        echo "$i) $tst"
        time python $tst $1 2> $tst.err.log > $tst.log
        if [[ `tail -1 $tst.log` == "ok" ]]
        then
                echo "Test $tst ok."
        else
                echo "Test $tst failed. See $tst.*.log"
        fi
	echo
done
