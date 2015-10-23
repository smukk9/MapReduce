import java.io.IOException;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LatestAccessMapper 
	extends Mapper<LongWritable, Text, KeyPair, Text> {
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
   
	  try{
		
		  String[] record = value.toString().split(" - - | - |\"");
		  if(record[1].contains("associate"))
			{
				
				String AccessTime = record[1].substring(11, 37);
				String IP = record[0];
				context.write(new KeyPair(IP, AccessTime), new Text(AccessTime));
				
			
		}else{
			
			
			String AccessTime = record[1].substring(1, 27);
			String IP = record[0];
			context.write(new KeyPair(IP, AccessTime), new Text(AccessTime));
		}
			

		 
	 }catch(Exception e){
		 System.out.println();
	 }
    }
}

