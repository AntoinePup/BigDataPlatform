package ecp.Lab1.TFIDF;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Tfidf3Reducer extends Reducer<Text, Text, Text, Text> {
		 
		 @Override
	      public void reduce(Text key, Iterable<Text> values, Context context)
	              throws IOException, InterruptedException {
			 HashSet<String> hashset = new HashSet<String>();
			 Double totalDocs = 0.00;
			 String word = key.toString();
			 String doc = new String();
			 Double wordCountPerDoc = 0.00;
			 Integer frequence = 0;
			 Integer docsPerWord = 0;
			 Double tfidf = 0.00;
			 ArrayList<String> cache = new ArrayList<String>();
			 
			 for (Text value : values){
				 doc = value.toString().split(",")[0];
				 docsPerWord++;
				 cache.add(value.toString());
				 if(!hashset.contains(doc)){
		    		 hashset.add(doc);
		    		 totalDocs++;
		    	 }
			 }
			 
			 for (String val : cache){
				 doc = val.split(",")[0];
				 frequence = Integer.parseInt(val.split(",")[1]);
				 wordCountPerDoc = Double.parseDouble(val.split(",")[2]);
				 
				 tfidf = (frequence/wordCountPerDoc)+Math.log(totalDocs/docsPerWord);
				 
				 context.write(new Text(word+","+doc), new Text(tfidf.toString()));
			 }
			 
			 
			 
	      }
	}