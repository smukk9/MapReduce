/**
 * The reducer class for step1 matrix multipilcation
 * @author Elham Buxton, University of Illinois at Springfield
 */
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class Step1Reducer extends
    Reducer<IntWritable, Text, Text, DoubleWritable> {

  public void reduce(IntWritable key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    //Separating the records from matrix A and Matrix B
    ArrayList<String> ARecords = new ArrayList<String>();
    ArrayList<String> BRecords = new ArrayList<String>();

    for (Text value : values) {
      if (value.toString().startsWith("A"))
        ARecords.add(value.toString().substring(2));
      else
        BRecords.add(value.toString().substring(2));
    }
    // For each value in ARecords, say (A,i,𝑎_𝑖𝑘) and 
    //each value in BRecords, say (B, j, 𝑏_𝑘𝑗) , emit the outkey= i, j
    // and the outvalue= 𝑎_𝑖𝑘×𝑎_𝑘𝑖)
    String outKey = "";
    for (String record1 : ARecords) {
      for (String record2 : BRecords) {
        outKey = "";
        StringTokenizer st1 = new StringTokenizer(record1, ",");
        outKey = st1.nextToken() + ",";
        double aik = Double.parseDouble(st1.nextToken());
        StringTokenizer st2 = new StringTokenizer(record2, ",");
        outKey += st2.nextToken() + ",";
        double bkj = Double.parseDouble(st2.nextToken());
        context.write(new Text(outKey), new DoubleWritable(aik * bkj));
      }
    }

  }
}
