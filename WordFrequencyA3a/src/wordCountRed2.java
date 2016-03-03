import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class wordCountRed2 extends
    Reducer<IntWritable, Text,Text,IntWritable> {
  public void reduce(IntWritable key, Iterable<Text>values, Context context)
      throws IOException, InterruptedException {
   
	  
	  
	  for(Text value: values){
		  
		  
		  context.write(value, key );
		  
	  }
	  
	  
    
  }
}

