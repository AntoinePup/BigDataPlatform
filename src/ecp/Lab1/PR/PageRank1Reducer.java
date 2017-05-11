package ecp.Lab1.PR;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRank1Reducer extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		String line = (0.85/PageRankDriver.nodes.size()) + ";";
		
		for (Text value: values){
			line+=value.toString()+";";
		}
		line = line.substring(0, line.length()-1);
		
		context.write(key, new Text(line));
	}
}