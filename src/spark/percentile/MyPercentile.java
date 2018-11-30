package spark.percentile;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.collection.Seq;

public class MyPercentile extends UserDefinedAggregateFunction {
	
	public static enum PercentileMode {
		INTERPOLATION_C1,
		INTERPOLATION_C0,
		NEAREST_RANK;
	}
	
	private double[] percentiles;
	
	private StructType inputDataType;
	
	private StructType bufferSchema;
	
	private DataType returnDataType;
	
	PercentileMode percentileMethod;

	
	public MyPercentile(double[] percentiles, PercentileMode percentileMethod) {
		this.percentiles = percentiles;
		this.percentileMethod = percentileMethod;
		
		// input col is a double
		List<StructField> inputFields = new ArrayList<>();
		inputFields.add(DataTypes.createStructField("inputDouble", DataTypes.DoubleType, true));
		inputDataType = DataTypes.createStructType(inputFields);

		bufferSchema = new StructType(new StructField[] {
				new StructField("buffer",
						DataTypes.createArrayType(DataTypes.DoubleType), false,
						Metadata.empty()) });

		// Return an array, one value per percentile
		returnDataType = DataTypes.createArrayType(DataTypes.DoubleType);
	}
	
	@Override
	public StructType bufferSchema() {
		return bufferSchema;
	}
	
	@Override
	public StructType inputSchema() {
		return inputDataType;
	}

	@Override
	public DataType dataType() {
		 return returnDataType;
	}

	@Override
	public boolean deterministic() {
		return true;
	}

	@Override
	public void initialize(MutableAggregationBuffer buffer) {
	    buffer.update(0, new ArrayList<Double>());
	}



	// Merges two aggregation buffers and stores the updated buffer values back to `buffer1`
	@Override
	public void merge(MutableAggregationBuffer buffer, Row row) {
		// Get array 
		List<Double> arr = toList(buffer.getSeq(0));
		arr.addAll(scala.collection.JavaConversions.seqAsJavaList(row.getSeq(0)));
		
		buffer.update(0, arr);
	}

	@Override
	public void update(MutableAggregationBuffer buffer, Row input) {
		if (!input.isNullAt(0)) {
			List<Double> arr = toList(buffer.getSeq(0));
			arr.add(input.getDouble(0));
			buffer.update(0, arr);
		}
	}
	

	@Override
	public Object evaluate(Row buffer) {
		
		// Get array from buffer
		List<Double> arr = toList(buffer.getSeq(0));
		
		// Sort array
		Collections.sort(arr);
		
		// results
		double[] percentilesRes = new double[percentiles.length];
		
		
		int idx = 0;
		for ( double percentile : percentiles ) {
			
			switch(percentileMethod) {
				case INTERPOLATION_C1:
					percentilesRes[idx++] = percentileInterpolationC1(arr, percentile);
					break;
				case INTERPOLATION_C0:
					percentilesRes[idx++] = percentileInterpolationC0(arr, percentile);
					break;
				case NEAREST_RANK:
					percentilesRes[idx++] = percentileNearestRank(arr, percentile);
					break;
			}
		}
		
		return percentilesRes;
	}
	
	// https://en.wikipedia.org/wiki/Percentile
	// Method nearest rank
	public  double percentileNearestRank(List<Double> sorted, double percentile) {
		double rank = (sorted.size()) * percentile;
		int idx = (int) Math.ceil(rank) - 1;

		if (idx < 1) {
			return sorted.get(0);
		}
		if (idx >= sorted.size()) {
			return sorted.get(sorted.size() - 1);
		}

		return sorted.get(idx);
	}
	
	//https://en.wikipedia.org/wiki/Percentile (second variant)
	
	public  double percentileInterpolationC0(List<Double> sorted, double percentile)  {
		
		double rank = percentile * (sorted.size() + 1);
		int rankInt = (int) rank;
		double rest = rank%1;
		
		if ( sorted.size() == 0 ) {
			return -1.;
		}
		
		if ( rankInt < 1 ) {
			return sorted.get(0);
		}
		
		if ( rankInt > (sorted.size() -1) ) {
			return sorted.get(sorted.size() -1);
		}
		return sorted.get(rankInt-1) + rest * (sorted.get(rankInt) - sorted.get(rankInt-1));
	}
	
	public  double percentileInterpolationC1(List<Double> sorted, double percentile)  {

		double rank = percentile * (sorted.size() -1 ) + 1;
		int rankInt = (int) rank;
		double rest = rank%1;
		
		if ( sorted.size() == 0 ) {
			return -1.;
		}
		
		if ( rankInt < 1 ) {
			return sorted.get(0);
		}
		
		if ( rankInt > (sorted.size() -1) ) {
			return sorted.get(sorted.size() -1);
		}
		
		return sorted.get(rankInt-1) + rest * (sorted.get(rankInt) - sorted.get(rankInt-1));  
	}
	
	
 
	
	private List<Double> toList(Seq<Double> seq) {
		return new ArrayList<>(scala.collection.JavaConversions.seqAsJavaList(seq));
	}
	

}
