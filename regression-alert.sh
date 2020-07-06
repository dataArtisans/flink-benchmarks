#!/bin/bash

base_url='http://codespeed.dak8s.net:8000/timeline'

records_count=200 # history size to request and analyze
records_count_trend=10 # number of records to consider as median_trend, i.e. ignore regression if it lasts less # todo: use several grades?
median_trend_threshold=-2 # change threshold of trend median, percent
dev_ratio_threshold=6 # threshold for ratio of last deviation to MAD # todo: use percentile

function load_data () {
    local benchmark="$1"
    # exe=1,3 means flink and stateBackends
    curl -sS "$base_url/json/?exe=1,3&ben=$benchmark&env=2&revs=$records_count" | python -mjson.tool | grep '^\s*"\w\w\w\w\/\w\w\/\w\w \w\w:\w\w:\w\w\s*",' -A1 | grep '\.' | sed -r 's/^\s*([0-9]+)\..*/\1/g'
}

function lines () {
    wc -l $1 | cut -f1 -d' '
}

function median () {
    local file=$1
    local num_lines=$(lines $file)
    cat $file $file | sort -g | head -n$num_lines | tail -n1
}

function check_deviation () {
    local benchmark="$1"
    # echo "checking $benchmark"
    local val_file=$(mktemp)
    local val_file_trend=$(mktemp)

    load_data "$benchmark" > $val_file
    local last_value=$(head -n1 $val_file)
    head -n$records_count_trend  $val_file > $val_file_trend
    sort -o $val_file $val_file

    local num_vals=$(lines $val_file)
    if [ "$num_vals" -lt "$records_count" ]; then
        # echo "not enough data for $benchmark"
        return 0
    fi

    local median=$(median $val_file)
    local trend_median=$(median $val_file_trend)

    local dev_file=$(mktemp)
    while read val; do
        echo "$(($val-$median))" | tr -d '-' >> $dev_file
    done < $val_file  

    local median_dev=$(median $dev_file)

    if [ "$last_value" -gt "$median" ]; then
        local last_dev_abs=$(($last_value-$median))
    else
        local last_dev_abs=$(($median-$last_value))
    fi

    if [ "$median_dev" -eq 0 ]; then
        local last_dev_ratio=$(($last_dev_abs))
    else
        local last_dev_ratio=$(($last_dev_abs/$median_dev))
    fi

    if [ "$median" -eq 0 ]; then
        local median_trend=0
    else
        local median_trend=$(($trend_median*100/$median-100))
    fi

    if [ "$median_trend" -lt "$median_trend_threshold" ] || ( [ "$last_value" -lt "$median" ] && [ "$last_dev_ratio" -gt "$dev_ratio_threshold" ] ); then
        echo -e "<$base_url/#/?exe=1,3&ben=$benchmark&env=2&revs=$records_count&equid=off&quarts=on&extr=on|$benchmark>:\n \
    median=$median, last=$last_value, dev=$last_dev_abs, median of last $records_count_trend=$trend_median, trend change: $median_trend,\n \
    median dev=$median_dev, ratio=$last_dev_ratio, threshold=$dev_ratio_threshold"
    fi
}

check_deviation 'arrayKeyBy'
check_deviation 'asyncWait.ORDERED'
check_deviation 'asyncWait.UNORDERED'
check_deviation 'benchmarkCount'
check_deviation 'compressedFilePartition'
check_deviation 'globalWindow'
check_deviation 'listAdd.HEAP'
check_deviation 'listAdd.ROCKSDB'
check_deviation 'listAddAll.HEAP'
check_deviation 'listAddAll.ROCKSDB'
check_deviation 'listAppend.HEAP'
check_deviation 'listAppend.ROCKSDB'
check_deviation 'listGet.HEAP'
check_deviation 'listGet.ROCKSDB'
check_deviation 'listGetAndIterate.HEAP'
check_deviation 'listGetAndIterate.ROCKSDB'
check_deviation 'listUpdate.HEAP'
check_deviation 'listUpdate.ROCKSDB'
check_deviation 'mapAdd.HEAP'
check_deviation 'mapAdd.ROCKSDB'
check_deviation 'mapContains.HEAP'
check_deviation 'mapContains.ROCKSDB'
check_deviation 'mapEntries.HEAP'
check_deviation 'mapEntries.ROCKSDB'
check_deviation 'mapGet.HEAP'
check_deviation 'mapGet.ROCKSDB'
check_deviation 'mapIsEmpty.HEAP'
check_deviation 'mapIsEmpty.ROCKSDB'
check_deviation 'mapIterator.HEAP'
check_deviation 'mapIterator.ROCKSDB'
check_deviation 'mapKeys.HEAP'
check_deviation 'mapKeys.ROCKSDB'
check_deviation 'mapPutAll.HEAP'
check_deviation 'mapPutAll.ROCKSDB'
check_deviation 'mapRebalanceMapSink'
check_deviation 'mapRemove.HEAP'
check_deviation 'mapRemove.ROCKSDB'
check_deviation 'mapSink'
check_deviation 'mapUpdate.HEAP'
check_deviation 'mapUpdate.ROCKSDB'
check_deviation 'mapValues.HEAP'
check_deviation 'mapValues.ROCKSDB'
check_deviation 'networkBroadcastThroughput'
check_deviation 'networkLatency1to1'
check_deviation 'networkSkewedThroughput'
check_deviation 'networkThroughput'
check_deviation 'networkThroughput.1,100ms'
check_deviation 'networkThroughput.100,100ms'
check_deviation 'networkThroughput.100,100ms,SSL'
check_deviation 'networkThroughput.100,1ms'
check_deviation 'networkThroughput.1000,100ms'
check_deviation 'networkThroughput.1000,100ms,OpenSSL'
check_deviation 'networkThroughput.1000,100ms,SSL'
check_deviation 'networkThroughput.1000,1ms'
check_deviation 'networkThroughputCompressed'
check_deviation 'readFileSplit'
check_deviation 'readFiles.txt-100-1000-10'
check_deviation 'readFiles.txt-1000-100-10'
check_deviation 'remoteRebalance'
check_deviation 'serializerAvro'
check_deviation 'serializerAvroReflect'
check_deviation 'serializerHeavyString'
check_deviation 'serializerKryo'
check_deviation 'serializerKryoWithoutRegistration'
check_deviation 'serializerPojo'
check_deviation 'serializerPojoWithoutRegistration'
check_deviation 'serializerRow'
check_deviation 'serializerTuple'
check_deviation 'sessionWindow'
check_deviation 'slidingWindow'
check_deviation 'stateBackends.FS'
check_deviation 'stateBackends.FS_ASYNC'
check_deviation 'stateBackends.MEMORY'
check_deviation 'stateBackends.ROCKS'
check_deviation 'stateBackends.ROCKS_INC'
check_deviation 'tumblingWindow'
check_deviation 'tupleKeyBy'
check_deviation 'twoInputMapSink'
check_deviation 'twoInputOneIdleMapSink'
check_deviation 'uncompressedFilePartition'
check_deviation 'uncompressedMmapPartition'
check_deviation 'valueAdd.HEAP'
check_deviation 'valueAdd.ROCKSDB'
check_deviation 'valueGet.HEAP'
check_deviation 'valueGet.ROCKSDB'
check_deviation 'valueUpdate.HEAP'
check_deviation 'valueUpdate.ROCKSDB'
