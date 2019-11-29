mkdir input
cp dataset_example.csv input
hadoop fs -mkdir -p input
hdfs dfs -put ./input/* input

mkdir downloads
cp pig-0.16.0.tar.gz downloads
cd downloads
#curl http://www-us.apache.org/dist/pig/pig-0.16.0/pig-0.16.0.tar.gz --output pig-0.16.0.tar.gz
tar -xzf pig-0.16.0.tar.gz

cd ..
./downloads/pig-0.16.0/bin/pig -x mapreduce script_lab3.pig
