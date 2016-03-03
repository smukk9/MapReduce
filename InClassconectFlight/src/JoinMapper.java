import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.io.*;

public class JoinMapper extends Mapper<LongWritable, Text, Text, TuplePair> {
  // Indices of join attributes for both relations
  public static final int A_JOINKEY_IND = 3;
  public static final int B_JOINKEY_IND = 2;
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String line = value.toString();
    TuplePair outValue = new TuplePair();
    int joinIndex;
    
    //retrieving the filename for which the input record comes from: orders.txt or customers.txt
    String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
    
    
    // initializing outevalue.relationName and joinIndex
    if (fileName.equals("A")) {
      outValue.setRelationName("A");
      joinIndex = A_JOINKEY_IND;
    } else {
      outValue.setRelationName("B");
      joinIndex = B_JOINKEY_IND;}
    // outkey= a comma-separated list of values of the join attributes
    // outTuple= a comma-separated lust of values of non-join attributes
    StringTokenizer st = new StringTokenizer(line, "\t");
    int index = 0;
    String outKey = "", outTuple = "";
    while (st.hasMoreTokens()) {
      String r = st.nextToken();
      index++;
      if (index==joinIndex)
        outKey += r;
      else
        outTuple += r + ",";
    }
    // removing the extra "," at the end
    outTuple = outTuple.substring(0, outTuple.length() - 1);
    outValue.setTuple(outTuple);
    context.write(new Text(outKey), outValue);
  }
}
