package ecp.Lab1.TFIDF;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Tfidf1Reducer extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		//Initialization of the variables

		HashMap<String, Integer> hashmap = new HashMap<String, Integer>();
		String file = new String();
		Integer count = 0;

		//We put every different filename into the hashmap and we increment the counter in order to keep the number of occurence in each files
		for (Text value : values){

			file = value.toString();

			if(hashmap.get(file)!=null){
				count = hashmap.get(file);
				count++;
				hashmap.put(file, ++count);
			} 
			else {	    			 
				hashmap.put(file,1);
			}
		}

		for (String value : hashmap.keySet()){
			context.write(new Text(key+";"+value), new Text(hashmap.get(value).toString()));
		}

	}
}