package ecp.Lab1.TFIDF;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Tfidf4Reducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {
	@Override
	public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		Double newTfidf = key.get();
		newTfidf = newTfidf*(-1);
		for (Text value : values){
			
			context.write(new Text(value), new DoubleWritable(newTfidf));
		}
	
	}
}