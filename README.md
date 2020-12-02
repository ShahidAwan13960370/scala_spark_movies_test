# movies-test

*Sample project to perform basic transformation on sample data using spark and scala, sample
data can be downloaded from http://files.grouplens.org/datasets/movielens/ml-1m.zip*

## Environment

Technically only sbt needs to be installed on the system but it is a good idea to setup a separate environment 
for this project, It is recommend to use sdkman to setup virutal environment.

```
sdk install java 8.0.272.hs-adpt
sdk install sbt 1.3.13
sdk install scala 2.11.12
```

There is already .sdkmanrc file available in the repo, once you have all the required sdk's ready just type
```
sdk env
```
This will create a virtual environment for you.

## Compilation and Packaging
In the project root directory type

```
sbt compile
sbt package
```

this will compile and create a jar file. which should be available at target/scala-2.12/movies-test_2.12-0.0.1.jar

## How to run

You can run it locally and also using spark cluster.

in order to run in a local spark cluster use the following spark submit command
```
$SPARK_HOME/bin/spark-submit --class com.test.movies.Transforms \
                    --master local 
                    target/scala-2.12/movies-test_2.12-0.0.1.jar 
                    /path/to/input_dir/ 
                    /path/to/output_dir/
```

It will expect following files in the input path
```
movies.dat
users.dat
ratings.dat
```

These files can be download from http://files.grouplens.org/datasets/movielens/ml-1m.zip.

After finishing the spark job, the transformed data can found in the output dir stored in parquet format.  
