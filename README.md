### Hot zone analysis
This task will perform a range join operation on a rectangle datasets and a point dataset. For each rectangle, the number of points located within the rectangle will be obtained. The hotter rectangle means that it include more points. So this task is to calculate the hotness of all the rectangles. Associated files are "~/scr/main/scala/HotzoneAnalysis.scala" and "~/scr/main/scala/HotzoneUtils.scala".


### Hot cell analysis
#### Description
This task will focus on applying spatial statistics to spatio-temporal big data in order to identify statistically significant spatial hot spots using Apache Spark. The topic of this task is from ACM SIGSPATIAL GISCUP 2016.

The Problem Definition page is here: [http://sigspatial2016.sigspatial.org/giscup2016/problem](http://sigspatial2016.sigspatial.org/giscup2016/problem) 
The Submit Format page is here: [http://sigspatial2016.sigspatial.org/giscup2016/submit](http://sigspatial2016.sigspatial.org/giscup2016/submit)

#### Special requirement (different from GIS CUP)
As stated in the Problem Definition page, in this task, I implemented a Spark program to calculate the Getis-Ord statistic of NYC Taxi Trip datasets. We call it "**Hot cell analysis**"
To reduce the computation power needï¼Œwe made the following changes:
1. The input will be a monthly taxi trip dataset from 2009 - 2012. For example, "yellow\_tripdata\_2009-01\_point.csv", "yellow\_tripdata\_2010-02\_point.csv".
2. Each cell unit size is 0.01 * 0.01 in terms of latitude and longitude degrees.
3. We should use 1 day as the Time Step size. The first day of a month is step 1. Every month has 31 days.
4. We only need to consider Pick-up Location.
5. We don't use Jaccard similarity to check answer.


### Input parameters
1. Output path (Mandatory)
2. Task name: "hotzoneanalysis" or "hotcellanalysis"
3. Task parameters: (1) Hot zone (2 parameters): nyc taxi data path, zone path(2) Hot cell (1 parameter): nyc taxi data path
Example
```
test/output hotzoneanalysis src/resources/point-hotzone.csv src/resources/zone-hotzone.csv hotcellanalysis src/resources/yellow_tripdata_2009-01_point.csv
```


### Input data format
The main function/entrace is "scala.Entrance" scala file.

1. Point data: the input point dataset is the pickup point of New York Taxi trip datasets. The data format of this phase is the original format of NYC taxi trip. Find the data from the S3 bucket: [Data Systems Lab S3 Bucket](https://datasyslab.s3.amazonaws.com/index.html?prefix=nyctaxitrips/). I parsed it already.
2. Zone data (only for hot zone analysis): at "src/resources/zone-hotzone" of the template

#### Hot zone analysis
The input point data can be any small subset of NYC taxi dataset.

#### Hot cell analysis
The input point data is a monthly NYC taxi trip dataset (2009-2012) like "yellow\_tripdata\_2009-01\_point.csv"


### Output data format
#### Hot zone analysis
All zones with their count, sorted by "rectangle" string in an ascending order. 

```
"-73.795658,40.743334,-73.753772,40.779114",1
"-73.797297,40.738291,-73.775740,40.770411",1
"-73.832707,40.620010,-73.746541,40.665414",20
```

#### Hot cell analysis
The coordinates of top 50 hotest cells sorted by their G score in a descending order. Note, DO NOT OUTPUT G score.

```
-7399,4075,15
-7399,4075,29
-7399,4075,22
```


### Example answers
An example input and answer are put in "testcase" folder of the coding template


### How to debug your code in IDE
1. Use IntelliJ Idea with Scala plug-in or any other Scala IDE.
2. Replace the logic of User Defined Functions ST\_Contains and ST\_Within in SpatialQuery.scala.
3. Append ```.master("local[*]")``` after ```.config("spark.some.config.option", "some-value")``` to tell IDE the master IP is localhost.
3. In some cases, you may need to go to "build.sbt" file and change ```% "provided"``` to ```% "compile"``` in order to debug your code in IDE
4. Run your code in IDE
5. **You must revert Step 3 and 4 above and recompile your code before use spark-submit!!!**


### How to submit your code to Spark
If you are using the Scala template
1. Go to project root folder
2. Run ```sbt clean assembly```. You need to install sbt in order to run this command. Then you will get a "target" directory under root directory, where you can find a jar file.
3. Submit the jar to Spark using Spark command "./bin/spark-submit". A pseudo code example: ```./bin/spark-submit ~/ProjectRootFolder/target/scala-2.11/Hotspot-Analysis-Template-assembly-0.1.0.jar test/output hotzoneanalysis src/resources/point-hotzone.csv src/resources/zone-hotzone.csv hotcellanalysis src/resources/yellow_tripdata_2009-01_point.csv```
