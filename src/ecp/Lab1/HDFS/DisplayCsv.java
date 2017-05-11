package ecp.Lab1.HDFS;


import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;



public class DisplayCsv {

	public static void main(String[] args) throws IOException {
		
		
		Path filename = new Path("input/HDFS/isd-history.txt");
		
		//Open the file
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream inStream = fs.open(filename);	

		try{
			
			InputStreamReader isr = new InputStreamReader(inStream);
			BufferedReader br = new BufferedReader(isr);
			
			// read line by line
			String line = br.readLine();
			// start at line 23
			int line_nb = 1;
			while(line_nb < 23){
				line_nb = line_nb +1;
				line = br.readLine();
			}

			while (line !=null){
				System.out.println(line.substring(0,6)+ "," + line.substring(13,13+29) + "," + line.substring(43,43+2) + "," + line.substring(74,74+7));
				line = br.readLine();
			}
		}
		finally{
			//close the file
			inStream.close();
			fs.close();
		}

		
		
	}

}