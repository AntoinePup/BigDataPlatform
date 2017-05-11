package ecp.Lab1.TFIDF;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Tfidf2Mapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String word = value.toString().split(";")[0];
		String doc = value.toString().split(";")[1];
		String frequence = value.toString().split(";")[2];

		context.write(new Text(doc), new Text(word+";"+frequence));
	}
}