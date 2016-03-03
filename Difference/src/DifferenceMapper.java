import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;

public class DifferenceMapper extends
    Mapper<LongWritable, Text, Text, Text> {
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
 
	  String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
	    
	  
	  if(fileName.equalsIgnoreCase("A")){
		  context.write(value, new Text("A"));
	  }else if(fileName.equalsIgnoreCase("B"))
		  
		  context.write(value,new Text("B"));
	  }
	  
     
  
    }
  

