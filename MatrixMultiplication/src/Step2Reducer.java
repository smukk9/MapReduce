import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class Step2Reducer extends
    Reducer<Text, DoubleWritable, Text, DoubleWritable> {
  /**
   * The reduce function simply forwards the key and emits the summation of values
   */
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
      throws IOException, InterruptedException {
    double sum = 0.0;
    for (DoubleWritable value : values)
      sum += value.get();

    context.write(key, new DoubleWritable(sum));
  }
}
