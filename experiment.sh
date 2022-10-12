#!/usr/bin/env bash
# set -x
# ./gradlew :core:test --tests '*.testFastTopicDeletionAndRecreation'|tee test.log
# ./sanity-check.sh 

# ./gradlew :core:test --tests '*.testLogCleaningAfterDeletion'|tee test.log
#./sanity-check.sh 


rm -rf /tmp/kafka-*
./gradlew :core:test --tests '*.testCorruptedLeaderEpochCheckpointOnLeader'|grep LLWW3


for partitionDir in /tmp/kafka-*/test-0; do
    logDir=$(dirname $partitionDir)
    echo "---------"
    echo $logDir
    cat $logDir/meta.properties|grep broker
    
    logFile="$logDir/*.log"
    ./bin/kafka-dump-log.sh --deep-iteration --files $partitionDir/*.log 2>&1|grep -v SLF
done

# for dir in $(ls /tmp/kafka-*); do
    
#     ./bin/kafka-dump-log.sh 
# done
