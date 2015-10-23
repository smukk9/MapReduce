import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxBirthYearMapper 
	extends Mapper<LongWritable, Text, KeyPair, IntWritable> {
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
   
   //Extracting the fields
	  String[] record = value.toString().split(" ");
	  
   /* 
    * Emitting The lastname and birth year from the record
    * the records are in this format: lastname  firstname birthyear-month-day , e.g., "Jones Zeke 2001-DEC-12"
    * The mapper emits a pair of lastname and birth year as key and the birth year as value, e.g. key= Keypair(Jones,2001), value=2001  
   */ 
	 if (record.length == 3)
	 {
		 String lastname = record[0];
		 int birthyear = Integer.parseInt(record[2].substring(0,4));
		 context.write(new KeyPair(lastname, birthyear), new IntWritable(birthyear));
	 
	 }
    }
}
