package ecp.Lab1.WordCount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable ONE = new IntWritable(1);
	private Text word = new Text();

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		for (String token: value.toString().replaceAll("[^0-9A-Za-z]"," ").split("\\s+")) {
			token = token.toLowerCase();
			if(!token.equals(" ") && !token.equals("")){
				word.set(token);
				context.write(word, ONE);
			}
		}
	}
}

