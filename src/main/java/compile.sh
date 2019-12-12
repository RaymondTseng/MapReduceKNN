export JAVA_HOME=/usr/local/jdk1.8.0_101
mkdir build
mkdir output1
mkdir output4
hadoop fs -mkdir knn
hadoop fs -mkdir knn/input
hadoop fs -put samples.txt knn/input
rm ./build/*
javac -cp .:/opt/cloudera/parcels/CDH/lib/hadoop/*:/opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/*      MapReduceKNN.java -d build -Xlint
rm MapReduceKNN.jar
jar cvf MapReduceKNN.jar -C build/ .
hadoop fs -rm -r knn/output1
rm ./output1/*
hadoop fs -rm -r knn/output2
hadoop fs -rm -r knn/output3
hadoop fs -rm -r knn/output4
rm ./output4/*
hadoop jar MapReduceKNN.jar MapReduceKNN knn/input knn/output1 knn/output2 knn/output3 knn/output4 $1 $2 $3 $4
hadoop fs -copyToLocal knn/output4/* ./output4/
