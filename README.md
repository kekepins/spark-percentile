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

In spark this can be done with UserDefinedAggregateFunction, java code can be see [here] ()
