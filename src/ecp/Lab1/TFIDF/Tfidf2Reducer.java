package ecp.Lab1.TFIDF;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Tfidf2Reducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		String doc = key.toString();
		Integer wordCountPerDoc = 0;
		Integer frequence = 0;
		String word = new String();
		ArrayList<String> cache = new ArrayList<String>();
		for (Text value : values){
			frequence = Integer.parseInt(value.toString().split(";")[1]);
			cache.add(value.toString());
			wordCountPerDoc+=frequence;
		}

		for (String val : cache){
			word = val.split(";")[0];
			frequence = Integer.parseInt(val.split(";")[1]);
			context.write(new Text(word+","+doc), new Text(frequence+","+wordCountPerDoc));
		}
	}
}