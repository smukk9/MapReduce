import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class JoinReducer extends Reducer<Text, TuplePair, NullWritable, Text> {

  public void reduce(Text joinfield, Iterable<TuplePair> values, Context context)
      throws IOException, InterruptedException {

    //An array to store the tuples coming from the customer relation
	  ArrayList<String> CRecords = new ArrayList<String>();
	  
	//an array to store the tuples coming from the orders relation
	  ArrayList<String> ORecords = new ArrayList<String>();
	 
   
    //Separating the tuples associated with customers and orders into CRecords and ORecords
    for (TuplePair tp : values) {

      if (tp.getRelationName().equals("A"))
        CRecords.add(tp.getTuple());
      else
        ORecords.add(tp.getTuple());


    }
   //Emitting all the combinations  of the tuples in CRecords and ORecords
    String outValue;
    for (String record1 : CRecords)
      for (String record2 : ORecords) {
        outValue = joinfield + "," + record1.toString() + "," + record2.toString();
        context.write(NullWritable.get(), new Text(outValue));
      }
  }
}
