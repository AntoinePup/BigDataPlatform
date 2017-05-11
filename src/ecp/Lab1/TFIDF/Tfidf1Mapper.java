package ecp.Lab1.TFIDF;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class Tfidf1Mapper extends Mapper<LongWritable, Text, Text, Text> {
	private Text word = new Text();
	private Text fileName = new Text();


	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		for (String token: value.toString().replaceAll("[^0-9A-Za-z]"," ").split("\\s+")){
			String nameOfFile = ((FileSplit) context.getInputSplit()).getPath().getName();
			fileName = new Text(nameOfFile);
			token = token.toLowerCase();
			word.set(token);
			if(!word.equals("") && word.getLength()!=0){
				context.write(word, fileName);
			}
		}
	}
}	