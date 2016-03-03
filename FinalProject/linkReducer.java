import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class linkReducer extends
Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
  throws IOException, InterruptedException {
	for(IntWritable value: values){
		context.write(key, (IntWritable) value);
	}

			
}
}
