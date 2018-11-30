package spark.percentile;

import static org.apache.spark.sql.functions.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import spark.percentile.MyPercentile.PercentileMode;

/**
 * Test Percentile Wikipedia with spark
 * 
 * https://en.wikipedia.org/wiki/Percentile  
 *
 */
public class TestWikipedia {
	
	private SparkSession sparkSession; 
	
	public static void main(String[] args) {
		
		
		TestWikipedia testWikipedia = new TestWikipedia();
		testWikipedia.init();
		
		// Test 1, Nearest rank example 1
		//-------------------------------
		testWikipedia.showPercentiles( new double[] { 0.05, 0.3, 0.4, 0.5, 1. }, new double[] {15, 20, 35, 40, 50});
		
		// Test 2, Nearest rank example 2
		//-------------------------------
		testWikipedia.showPercentiles( new double[] { 0.25, 0.5, 0.75, 1. }, new double[] {3, 6, 7, 8, 8, 10, 13, 15, 16, 20});
		
		// Test 3, Nearest rank example 3
		//-------------------------------
		testWikipedia.showPercentiles( new double[] { 0.25, 0.5, 0.75, 1. }, new double[]  {3, 6, 7, 8, 8, 9, 10, 13, 15, 16, 20});

		// Test 4, Interpolation between closest rank (C=1) example 1 [second variant]
		//----------------------------------------------------------------------------
		testWikipedia.showPercentiles( new double[] { 0.4 }, new double[]  {15, 20, 35, 40, 50});
		
		// Test 5, Interpolation between closest rank (C=1) example 2 
		//-----------------------------------------------------------
		testWikipedia.showPercentiles( new double[] { 0.75 }, new double[]  {1,2,3,4});


		// Test 6, Interpolation between closest rank (C=0) example 1 [third variant]
		//---------------------------------------------------------------------------
		testWikipedia.showPercentiles( new double[] { 0.05, 0.3, 0.4, 0.95 }, new double[]   {15, 20, 35, 40, 50});


	}
	
	public void init() {
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		
		sparkSession = SparkSession.builder().master("local[8]").appName("Test Percentile Wikipedia")
				.config("spark.sql.warehouse.dir", "file:///C:/temp/working").getOrCreate();
		
	}
	
	
	
	
	private void showPercentiles(double[] percentiles, double[] values) {
		
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
	}
	
	

	// Construct a dataset from an array
	private  Dataset<Row> fromArray(SparkSession spark, double[] arr) {
		StructField values = new StructField("data", DataTypes.DoubleType, true, Metadata.empty());
	    StructType structType = new StructType(new StructField[]{values});
	    List<Row> data = new ArrayList<Row>();
	    
	    for ( double val : arr ) {
	    	data.add( RowFactory.create(val));
	    }
	    return  spark.createDataFrame(data, structType);
	}
}
