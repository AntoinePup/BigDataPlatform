package ecp.Lab1.TFIDF;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Tfidf4Mapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String word = value.toString().split(";")[0].split(",")[0];
		String doc = value.toString().split(";")[0].split(",")[1];
		Double tfidf = (-1)*Double.parseDouble(value.toString().split(";")[1]);



		context.write(new DoubleWritable(tfidf), new Text(word+","+doc));
	}
}