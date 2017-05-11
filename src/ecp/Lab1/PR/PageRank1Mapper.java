package ecp.Lab1.PR;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRank1Mapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		if (value.toString().charAt(0) != '#') {
			String nodeA = value.toString().split("\t")[0];
			String nodeB = value.toString().split("\t")[1];
			context.write(new Text(nodeA), new Text(nodeB));
			
			//We add all the node to a hashset. Allows us to know the number of distinc nodes at the end as hashset do not accept duplicates.
			PageRankDriver.nodes.add(nodeA);
			PageRankDriver.nodes.add(nodeB);
		}


	}
}