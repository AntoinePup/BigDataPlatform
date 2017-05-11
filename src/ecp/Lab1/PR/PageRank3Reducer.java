package ecp.Lab1.PR;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRank3Reducer extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {
	@Override
	public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		Double newKey = key.get();
		newKey = newKey*(-1);
		for (Text value : values){
			
			context.write(new DoubleWritable(newKey), value);
		}
	
	}
}