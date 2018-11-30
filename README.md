# spark-percentile

From the wikipedia page about percentile: 
https://en.wikipedia.org/wiki/Percentile

Let's do some experiments with spark.

Spark has two built-in percentile implementations:
- percentile
- percentile_approx

Let's implement 3 new one described in wikipedia page:
* nearest rank
* interpolation C1
* interpolation C0

In spark this can be done with UserDefinedAggregateFunction, java code can be see [here](https://github.com/kekepins/spark-percentile/blob/master/src/spark/percentile/MyPercentile.java)


Now test this on various examples from wikipedia page [code here](https://github.com/kekepins/spark-percentile/blob/master/src/spark/percentile/TestWikipedia.java)

With code:

```java
// Register udf 
sparkSession.udf().register("percentileC1", new MyPercentile(percentiles, PercentileMode.INTERPOLATION_C1));
sparkSession.udf().register("percentileC0", new MyPercentile(percentiles, PercentileMode.INTERPOLATION_C0));
sparkSession.udf().register("percentileNearestRank", new MyPercentile(percentiles, PercentileMode.NEAREST_RANK));

// Get a dataset
Dataset<Row> ds = fromArray(sparkSession, values);
ds.show(false);
		
// Compute percentiles
ds = ds.select(
    	callUDF("percentileC1", col("data")).as("Percentile C1"),
    	callUDF("percentileC0", col("data")).as("Percentile C0"),
    	callUDF("percentileNearestRank", col("data")).as("Percentile Nearest Rank"),
    	callUDF("percentile_approx", col("data"), lit( percentiles) ).as("percentile_approx (spark builtin)"),
    	callUDF("percentile", col("data"), lit( percentiles) ).as("percentile (spark builtin)")
    		);
		
ds.show(false);

```

#### Test 1, Nearest rank example 1
|data|
| -- |
|15.0|
|20.0|
|35.0|
|40.0|
|50.0|

|Percentile C1          |Percentile C0           |Percentile Nearest Rank|percentile_approx (spark builtin)|percentile (spark builtin)|
|-----------------------|------------------------|-----------------------|---------------------------------|--------------------------|
|[7.25, 9.0, 14.5, 20.0]|[6.75, 9.0, 15.25, 20.0]|[7.0, 8.0, 15.0, 20.0] |[7.0, 8.0, 15.0, 20.0]           |[7.25, 9.0, 14.5, 20.0]   |



#### Test 2, Nearest rank example 2
#### Test 3, Nearest rank example 3
#### Test 4, Interpolation between closest rank (C=1) example 1 (second variant)
#### Test 5, Interpolation between closest rank (C=1) example 2 
#### Test 6, Interpolation between closest rank (C=0) example 1 (third variant)



