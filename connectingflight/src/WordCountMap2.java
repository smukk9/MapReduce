import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


//Receives key = word1,word2 and key as the frequency
public class WordCountMap2 extends
Mapper<IntWritable, Text, IntWritable, Text> {
public void map(LongWritable key, Text value, Context context)
  throws IOException, InterruptedException {
		       
	try{
		
	
		    String[] record = value.toString().split(" ");
		    String word = record[0] +""+record[1];
		   int Frequency = Integer.parseInt(record[2]);
		    
		    context.write(new IntWritable(Frequency), new Text(word));
		    
		    		  
		    		  
		    		  
		    	  }catch ( Exception e){
		    		  e.printStackTrace();
		    	  }
}
}
