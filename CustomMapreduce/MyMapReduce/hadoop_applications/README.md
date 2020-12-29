# How to run a Hadoop Application with our Setup on 5 VMs

## make Application.java file with your application

## create directory for appliction classes
mkdir application_classes

## compile Application.java file
javac -cp `hadoop classpath` -d application_classes Application.java

## make jar file with your application
jar -cvf application.jar -C application_classes/ .

## create input directory and put data in
mkdir input
### put files in
hdfs dfs -mkdir input
hdfs dfs -put input/your_file input

## run your application in hadoop
hadoop jar application.jar Application input output

## view output of application
hdfs dfs -cat output/part-r-00000