package ecp.Lab1.PR;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRank3Mapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		
		String[] values = value.toString().split(";");
		String currentNode = values[0];
		Double pageRank = Double.parseDouble(values[1]);
		pageRank = pageRank*(-1);
		
		
		context.write(new DoubleWritable(pageRank), new Text(currentNode));
	}
}