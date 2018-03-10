#!/usr/bin/env bash


DATASETS=(trucks-d brink-d tdrive-d)
trucks_time_offsets={2 2875}
tdrive_time_offsets={0 53328}
brink_time_offsets={2 20000}
CLUSTERING_EXP_RESULT=clustering_exps.txt
SPARE_EXP_RESULT=spare_exps.txt
CORES=(16 14 12 10 8 6 4 2)
CLUSTERING_IN_DIR=/user/faisal/datasets
CLUSTERING_OUT_DIR=/user/faisal/output


#===============================================Clustering Phase============================================

E=(0.000060) #eps for DBSCAN
M=(6) #min num of objs in a cluster
H=(87) #hdfs partitions
S=(87) #snapshot partitions
r=0 #use earth distance calc = false


rm ${CLUSTERING_EXP_RESULT}
touch ${CLUSTERING_EXP_RESULT}
echo "dataset,e,m,cores,time(ms)" >> ${CLUSTERING_EXP_RESULT}

for dataset in ${DATASETS[@]}
do
for core in ${CORES[@]}
do
for m in ${M[@]}
do
for e in ${E[@]}
do
    start=$(date +%s%3N)
    echo $dataset,${e},${m}
    hdfs dfs -rm -r ${CLUSTERING_OUT_DIR}/${dataset}/clusters-e${e}-m${m}
    spark-submit --master yarn --deploy-mode client --executor-cores ${core} \
        jars/TrajectoryMining-0.0.1-SNAPSHOT.jar input.MainApp e=${e} m=${m} \
        input=${CLUSTERING_IN_DIR}/${dataset} output=${CLUSTERING_OUT_DIR}/${dataset}
    end=$(date +%s%3N)
    echo $dataset,${e},${m},${core},$(($end-$start))
    echo "$dataset,${e},${m},${core},$(($end-$start))" >> ${CLUSTERING_EXP_RESULT}
done
done
done
done

# ================================Trajectory Mining==============================


rm ${SPARE_EXP_RESULT}
touch ${SPARE_EXP_RESULT}
echo "dataset,k,l,m,g,c,cores,time(ms)" >> $SPARE_EXP_RESULT

CORES=(1 2 3 4 5 6 7 8)
#K=(90 180 270 360)
K=(180)
L=(180) #length of local consecutive pattern
M=(6)
G=(0) # gap
C=(115) # Num of clique miner partitions


for dataset in ${DATASETS[@]}
do
for c in ${C[@]}
do
for k in ${K[@]}
do
for l in ${L[@]}
do
for m in ${M[@]}
do
for e in ${E[@]}
do
for g in ${G[@]}
do
for core in ${CORES[@]}
do
    start=$(date +%s%3N)
    echo ${dataset},${k},${l},${m},${g},${c}
    spark-submit --master yarn --deploy-mode client --executor-cores ${core} \
        jars/TrajectoryMining-0.0.1-SNAPSHOT.jar apriori.MainSP k=${k} l=${l} m=${m} g=${g} c=${c}\
        input=${CLUSTERING_OUT_DIR}/${dataset}/clusters-e${e}-m${m}
    end=$(date +%s%3N)
    echo ${dataset},${k},${l},${m},${g},${c},$(($end-$start))
    echo "${dataset},${k},${l},${m},${g},${c},${core},$(($end-$start))" >> $SPARE_EXP_RESULT
done
done
done
done
done
done
done
done


java -cp target/DistributedConvoy-0.0.1-SNAPSHOT.jar mapreduce.ConvoyJobNlognMRSplits /user/faisal/datasets/trucks-d /user/faisal/output/dcm-mr/trucks-d 6 180 0.00006
