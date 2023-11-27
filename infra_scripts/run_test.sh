# Test Convenience Script
# CS 165
# Contact: Wilson Qin

# note this should be run from base project folder as `./infra_scripts/run_test 01`
# note this should be run inside the docker container

# If a container is already successfully running after `make startcontainer outputdir=<ABSOLUTE_PATH1> testdir=<ABSOLUTE_PATH2>`
# This endpoint takes a `test_id` argument, from 01 up to 43,
#     runs the corresponding generated test DSLs
#    and checks the output against corresponding EXP file.

test_id=$1
output_dir=${2:-'/cs165/infra_outputs'}

echo "Running test # $test_id"

cd /cs165/src
# collect the client output for this test case by test_id
start=`date +%s%N`
./client < /cs165/staff_test/test${test_id}gen.dsl 2> ${output_dir}/test${test_id}gen.out.err 1> ${output_dir}/test${test_id}gen.out
end=`date +%s%N`
echo Execution time was `expr $end - $start` nanoseconds.

if [[ $test_id == 18 ]] || [[ $test_id == 19 ]] 
then
    echo Test $test_id is a performance test. Checking its speedup...
    prev_time=`cat tmp.prev_time`
    curr_time=`expr $end - $start`
    speedup=`printf "%.4f\n" $((10**4 * curr_time/prev_time))e-4`
    ths=2.0
    echo Speedup is $speedup. Checking whether it satisfies the performance requirements for threshold of $ths\...
    awk -v n1="$speedup" -v n2="$ths" 'BEGIN { printf "" (n1>=n2?  "Yes, it does. Success! [\033[42mok\033[0m]\n"  : "No, it does not. Failure! [\033[31mfail\033[0m]\n")}'
    rm tmp.prev_time

    echo Now, checking whether it also outputs correct results...
fi

if [[ $test_id == 34 ]] || [[ $test_id == 37 ]] || [[ $test_id == 40 ]] || [[ $test_id == 43 ]]  
then
    echo Test $test_id is a performance test. Checking its speedup...
    prev_time=`cat tmp.prev_time`
    curr_time=`expr $end - $start`
    speedup=`printf "%.4f\n" $((10**4 * prev_time/curr_time))e-4`
    ths=1.5
    echo Speedup is $speedup. Checking whether it satisfies the performance requirements for threshold of $ths\...
    awk -v n1="$speedup" -v n2="$ths" 'BEGIN { printf "" (n1>=n2?  "Yes, it does. Success! [\033[42mok\033[0m]\n"  : "No, it does not. Failure! [\033[31mfail\033[0m]\n")}'
    rm tmp.prev_time

    echo Now, checking whether it also outputs correct results...
fi

if [[ $test_id == 35 ]] || [[ $test_id == 38 ]] || [[ $test_id == 41 ]] || [[ $test_id == 44 ]]   
then
    echo Test $test_id is a performance test. Checking its speedup...
    prev_time=`cat tmp.prev_time`
    curr_time=`expr $end - $start`
    speedup=`printf "%.4f\n" $((10**4 * prev_time/curr_time))e-4`
    ths=1.01
    echo Speedup is $speedup. Checking whether it satisfies the performance requirements for threshold of $ths\...
    awk -v n1="$speedup" -v n2="$ths" 'BEGIN { printf "" (n1>=n2?  "Yes, it does. Success! [\033[42mok\033[0m]\n"  : "No, it does not. Failure! [\033[31mfail\033[0m]\n")}'
    rm tmp.prev_time

    echo Now, checking whether it also outputs correct results...
fi

if [[ $test_id == 16 ]] || [[ $test_id == 18 ]] || [[ $test_id == 33 ]] || [[ $test_id == 34 ]] || [[ $test_id == 36 ]] || [[ $test_id == 37 ]] || [[ $test_id == 39 ]] || [[ $test_id == 40 ]] || [[ $test_id == 42 ]] || [[ $test_id == 43 ]]
then
    prev_time=`expr $end - $start`
    echo $prev_time > tmp.prev_time
fi

cd /cs165
# run the "comparison" script for comparing against expected output for test_id
./infra_scripts/verify_output_standalone.sh $test_id ${output_dir}/test${test_id}gen.out /cs165/staff_test/test${test_id}gen.exp ${output_dir}/test${test_id}gen.cleaned.out ${output_dir}/test${test_id}gen.cleaned.sorted.out
