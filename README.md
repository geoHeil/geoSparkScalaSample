# geospatial processing in spark
A mini sample project to demonstrate geospatial processing in geoSpark http://geospark.datasyslab.org/ in scala using WKT text files as input.

It is partially based on https://github.com/DataSystemsLab/GeoSpark/wiki/GeoSpark-Tutorial and https://github.com/DataSystemsLab/GeoSpark/tree/2adce0c1c13af172f9be6c3cd0cda1431c74d0b8/src/main/java/org/datasyslab/geospark/showcase

**Overview**

We will load Data from two files (Boxes and Polyons) and perform a geospatial join.

**getting started**

```
git clone https://github.com/geoHeil/geoSparkScalaSample.git
cd geoSparkScalaSample
sbt run
```

**purpose of this project**
- show a scala sample of using geoSpark
- get input how to create a more scala native `CustomInputMapperWKT`
- help me to resolve all the TODOS
