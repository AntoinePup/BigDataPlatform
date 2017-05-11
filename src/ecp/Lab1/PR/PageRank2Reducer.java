package ecp.Lab1.PR;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRank2Reducer extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		String line = new String();
		Double sum = 0.00;
		
		for (Text value:values){
			if(value.toString().charAt(0)=='#'){
				line+= value.toString().substring(1);
			}
			else {
				Double pageRank = Double.parseDouble(value.toString().split(";")[0]);
				Integer nbNodes = Integer.parseInt(value.toString().split(";")[1]);
				
				sum+=(pageRank/nbNodes);
			}
		}
		
		Double newPageRank = 0.85*sum+(1-0.85);
		
		context.write(key, new Text(newPageRank.toString()+";"+line));
	
	}
}