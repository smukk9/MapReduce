/**
 * The mapper class for step2 matrix multipilcation
 * @author Elham Buxton, University of Illinois at Springfield
 */
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class Step2Mapper extends
    Mapper<LongWritable, Text, Text, DoubleWritable> {

  /**
   * The map function for step 2 of matrix multiplication receives a record 
   * in form of (i,j,v) and emits the outkey = i,j and outValue = v
   */
	public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
	StringTokenizer record = new StringTokenizer(value.toString(), ",");
    String outkey = record.nextToken() + "," + record.nextToken();
    double outvalue = Double.parseDouble(record.nextToken());
    context.write(new Text(outkey), new DoubleWritable(outvalue));
  }
}
