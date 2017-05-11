package ecp.Lab1.WordCount;

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountDriver extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
	      System.out.println(Arrays.toString(args));
	      int res = ToolRunner.run(new Configuration(), new WordCountDriver(), args);
	      
	      System.exit(res);
	   }

	   @Override
	   public int run(String[] args) throws Exception {
	      System.out.println(Arrays.toString(args));
	      Job job = new Job(getConf(), "WordCountMapper");
	      job.setJarByClass(WordCountDriver.class);
	      job.setOutputKeyClass(Text.class);
	      job.setOutputValueClass(IntWritable.class);

	      job.setMapperClass(WordCountMapper.class);
	      job.setReducerClass(WordCountReducer.class);

	      job.setInputFormatClass(TextInputFormat.class);
	      job.setOutputFormatClass(TextOutputFormat.class);

	      
	      FileInputFormat.addInputPath(job, new Path("input/WordCount/")); 
	      Path outputPath = new Path("output/WordCount/");
	      FileOutputFormat.setOutputPath(job, outputPath);
	      FileSystem hdfs = FileSystem.get(getConf());
		  if (hdfs.exists(outputPath)){
		      hdfs.delete(outputPath, true);
		  }
	      
	      //FileInputFormat.addInputPath(job, new Path(args[0]));
	      //FileOutputFormat.setOutputPath(job, new Path(args[1]));

	      job.waitForCompletion(true);
	      
	      System.out.println("Total number of words : "+WordCountReducer.wordCount);
	      
	      return 0;
	   }
}
