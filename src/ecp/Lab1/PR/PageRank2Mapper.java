package ecp.Lab1.PR;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRank2Mapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		
		String[] values = value.toString().split(";");
		String currentNode = values[0];
		String pageRank = values[1];
		String[] nodes = Arrays.copyOfRange(values, 2, values.length);
		String line = new String();
		
		
		for (String node:nodes){
			context.write(new Text(node), new Text(pageRank+";"+nodes.length));
			line+=node+";";
		}
		if(line.length()>0){
			line = line.substring(0, line.length()-1);
		}
		context.write(new Text(currentNode), new Text("#"+line));
		//System.out.println("Map 2 :"+currentNode+" Page rank : "+pageRank);
	}
}