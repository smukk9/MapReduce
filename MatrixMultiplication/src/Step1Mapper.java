/**
 * The mapper class for step1 matrix multipilcation
 * @author Elham Buxton, University of Illinois at Springfield
 *
 */
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.io.*;
public class Step1Mapper extends Mapper<LongWritable, Text, IntWritable, Text> {

  @Override
	public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String line = value.toString();
    
  //retrieving the filename for which the input record comes from: A.txt or B.txt
    String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
    
    
    String outValue;
    int joinIndex,outKey=0;    
    
    //initializing the outValue and joinIndex
    if (fileName.equals("A.txt")){
      outValue = "A";
      //The first matrix is joined on its second attribute (The column number)
      joinIndex = 2;
    }
    else{
    	outValue="B";
    	//The second Matrix is joined on its first attribute (The row number)
    	joinIndex=1;
    }
    int index =0;
    
    /*Parsing the input tuple, if the tuple comes from matrix A emit the column number as the key
     *else emit the row number as the key. Just like join, the outValue is the name of the matrix
     *plus the rest of the attributes (other than join index)
     */
    StringTokenizer record = new StringTokenizer(line, ",");
    while (record.hasMoreTokens()) {
    	String r = record.nextToken();
        index++;
        if (index == joinIndex)
          outKey = Integer.parseInt(r);
        else
          outValue += "," + r;
      }
    context.write(new IntWritable(outKey), new Text(outValue));
  }
}
