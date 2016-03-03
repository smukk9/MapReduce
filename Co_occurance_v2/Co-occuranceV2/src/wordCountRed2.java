import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class wordCountRed2 extends
    Reducer<Text, IntWritable, Text, IntWritable> {
  public void reduce(IntWritable key, Text values, Context context)
      throws IOException, InterruptedException {
   
	  
	  
    context.write(values, key );
  }
}

