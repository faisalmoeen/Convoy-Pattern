#!/usr/bin/env bash


DATASETS=(trucks-d brink-d tdrive-d)
declare -A time_offsets
time_offsets[trucks-d_start]=2
time_offsets[trucks-d_end]=2875
time_offsets[brink-d_start]=2
time_offsets[brink-d_end]=20000
time_offsets[tdrive-d_start]=0
time_offsets[tdrive-d_end]=53328
DCM_MR_EXP_RESULT=dcm_mr_exps.txt
CORES=(16 14 12 10 8 6 4 2)
IN_DIR=/user/faisal/datasets
OUT_DIR=/user/faisal/output


#===============================================DCM_MR============================================

E=(0.000060) #eps for DBSCAN
M=(6) #min num of objs in a cluster
K=(180)


rm ${DCM_MR_EXP_RESULT}
touch ${DCM_MR_EXP_RESULT}
echo "dataset,e,m,cores,time(ms)" >> ${DCM_MR_EXP_RESULT}

for dataset in ${DATASETS[@]}
do
for m in ${M[@]}
do
for e in ${E[@]}
do
for k in ${K[@]}
do
    start=$(date +%s%3N)
    echo $dataset,${e},${m},${k}
    hadoop jar target/DistributedConvoy-0.0.1-SNAPSHOT.jar mapreduce.ConvoyJobNlognMRSplits ${IN_DIR}/${dataset} ${OUT_DIR}/${dataset} \
    ${m} ${k} ${e} ${time_offsets[${dataset}_start]} ${time_offsets[${dataset}_end]}
    end=$(date +%s%3N)
    echo $dataset,${e},${m},${k},${core},$(($end-$start))
    echo "$dataset,${e},${m},${k},${core},$(($end-$start))" >> ${DCM_MR_EXP_RESULT}
done
done
done
done


java -cp target/DistributedConvoy-0.0.1-SNAPSHOT.jar mapreduce.ConvoyJobNlognMRSplits /user/faisal/datasets/trucks-d /user/faisal/output/dcm-mr/trucks-d
hadoop jar target/DistributedConvoy-0.0.1-SNAPSHOT.jar mapreduce.ConvoyJobNlognMRSplits /user/faisal/datasets/trucks-d /user/faisal/output/trucks-d/DCM 6 180 0.00006 2 2875