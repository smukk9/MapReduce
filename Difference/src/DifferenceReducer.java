import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class DifferenceReducer extends
    Reducer<Text, Text, Text, NullWritable> {
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
   int r =0 , s = 0;
   for(Text v : values){
	   if(v.toString().equals("A"))
		   r++;
	   else if(v.toString().equals("B"))
		   s++;
   }
   int diff = r-s;
   for(int i =0;i<diff ;i++){
	   context.write(key, NullWritable.get());
   }
  }
}
