package ecp.Lab1.HDFS;


import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;



public class LineCount {

	public static void main(String[] args) throws IOException {
		
		
		Path filename = new Path("input/HDFS/arbres.csv");
		
		//Open the file
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream inStream = fs.open(filename);

		int nb_lines = 0;	

		try{
			
			InputStreamReader isr = new InputStreamReader(inStream);
			BufferedReader br = new BufferedReader(isr);
			
			// read line by line
			String line = br.readLine();
			while (line !=null){
				String strar[] = line.split(";");
            		System.out.println(strar[5] + "," + strar[6]);
				line = br.readLine();
				nb_lines++;
			}
			System.out.println("Total number of lines : "+nb_lines);
		}
		finally{
			//close the file
			inStream.close();
			fs.close();
		}

		
		
	}

}