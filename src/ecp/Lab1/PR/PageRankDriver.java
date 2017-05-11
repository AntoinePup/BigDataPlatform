package ecp.Lab1.PR;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class PageRankDriver extends Configured implements Tool { 
	
	public static Set<String> nodes = new HashSet<String>();
	
	public static void main(String[] args) throws Exception {
	      System.out.println(Arrays.toString(args));
	      int res = ToolRunner.run(new Configuration(), new PageRankDriver(), args);
	      
	      System.exit(res);
	   }
	
	
	@Override
	   public int run(String[] args) throws Exception {
	      System.out.println(Arrays.toString(args));
	      System.out.println("Starting Page Rank 1 ...");
	      Job job = new Job(getConf(), "PageRank");
	      job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ";"); //We use ";" as a delimitor in the output file instead of tab
	      job.setJarByClass(PageRankDriver.class);
	      job.setOutputKeyClass(Text.class);
	      job.setOutputValueClass(Text.class);
	      
	      job.setMapperClass(PageRank1Mapper.class);
	      job.setReducerClass(PageRank1Reducer.class);

	      job.setInputFormatClass(TextInputFormat.class);
	      job.setOutputFormatClass(TextOutputFormat.class);

	      FileInputFormat.addInputPath(job, new Path("input/PageRank")); 
	      Path outputPath = new Path("output/PageRank/Processing1/");
	      FileOutputFormat.setOutputPath(job, outputPath);
	      FileSystem hdfs = FileSystem.get(getConf());
		  if (hdfs.exists(outputPath)){
		      hdfs.delete(outputPath, true);
		  }
		  job.waitForCompletion(true);
		  
		  System.out.println("Starting Page Rank 2 ...");
		  job = Job.getInstance(new Configuration(), "PageRank2");
	      job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ";");
	      job.setJarByClass(PageRankDriver.class);
	      job.setOutputKeyClass(Text.class);
	      job.setOutputValueClass(Text.class);
	      
	      job.setMapperClass(PageRank2Mapper.class);
	      job.setReducerClass(PageRank2Reducer.class);

	      job.setInputFormatClass(TextInputFormat.class);
	      job.setOutputFormatClass(TextOutputFormat.class);

	      FileInputFormat.addInputPath(job, new Path("output/PageRank/Processing1/")); 
	      Path outputPath2 = new Path("output/PageRank/Processing2/");
	      FileOutputFormat.setOutputPath(job, outputPath2);
	      if (hdfs.exists(outputPath2)){
		      hdfs.delete(outputPath2, true);
		  }

	      job.waitForCompletion(true);
	      
	      System.out.println("Starting Page Rank 3 ...");
	      
	      job = Job.getInstance(new Configuration(), "PageRank3");
	      job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ";");
	      job.setJarByClass(PageRankDriver.class);
	      job.setOutputKeyClass(DoubleWritable.class);
	      job.setOutputValueClass(Text.class);
	      
	      job.setMapperClass(PageRank3Mapper.class);
	      job.setReducerClass(PageRank3Reducer.class);

	      job.setInputFormatClass(TextInputFormat.class);
	      job.setOutputFormatClass(TextOutputFormat.class);

	      FileInputFormat.addInputPath(job, new Path("output/PageRank/Processing2/")); 
	      Path outputPath3 = new Path("output/PageRank/Processing3/");
	      FileOutputFormat.setOutputPath(job, outputPath3);
	      if (hdfs.exists(outputPath3)){
		      hdfs.delete(outputPath3, true);
		  }

	      job.waitForCompletion(true);
	      
	      
	      System.out.println("Done");
	      
	      return 0;
	   }
	 
}
