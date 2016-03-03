import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


//Receives key = word1,word2 and value as the frequency
public class WordCountMap2 extends
Mapper<Text, IntWritable, IntWritable, Text> {
public void map(Text key, IntWritable value, Context context)
  throws IOException, InterruptedException {
		       
	try{
		
	
		   
		    context.write(value, key);
		    	  
		    		  
		    	  }catch ( Exception e){
		    		  e.printStackTrace();
		    	  }
}
}
