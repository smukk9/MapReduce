import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;

import java.io.IOException;

import org.apache.hadoop.io.*;

public class OneStopFlightMapper extends Mapper<LongWritable, Text, Text, TuplePair> {
  // Indices of join attributes for both relations
  public static final int A_JOINKEY_IND = 5;
  public static final int B_JOINKEY_IND = 4;
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
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
    String [] tokens = value.toString().split(",");
    if(!(tokens[0]=="year")){
    String [] records = {tokens[5],tokens[7],tokens[9],tokens[16],tokens[17]};
   
    int index = 0;
    String outKey = "", outTuple = "";
    outKey = tokens[0]+","+tokens[1]+","+tokens[2]+","+tokens[8]+",";
   for( String st: records) {
      String r = st;
      index++;
      if (index==joinIndex){
        outKey = outKey+r;
      }else{
        outTuple =outTuple+","+ r;
    }
   }
   outTuple = outTuple+ ","+records[joinIndex-1];
    // removing the extra "," at the end
    outTuple = outTuple.substring(1, outTuple.length());
    outValue.setTuple(outTuple);
    
    context.write(new Text(outKey), outValue);
  }
}
}


