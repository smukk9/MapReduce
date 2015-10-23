import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlightMapper 
	extends Mapper<LongWritable, Text, KeyPair, Text> {
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
   
   //Extracting the fields
	  String[] record = value.toString().split(",");
	  
   /* 
    * Emitting The lastname and birth year from the record
    * the records are in this format: lastname  firstname birthyear-month-day , e.g., "Jones Zeke 2001-DEC-12"
    * The mapper emits a pair of lastname and birth year as key and the birth year as value, e.g. key= Keypair(Jones,2001), value=2001  
   */ 
		 //if (record.length>=15)
		 //{
			 if(isNumeric(record[9]) && isNumeric(record[14])){
				 int flightNo = 0;
				 flightNo = Integer.parseInt(record[9]);		
				 int arrivalDelay = Integer.parseInt(record[14]);
				 
				 context.write(new KeyPair(flightNo, arrivalDelay), value);
			 }
		//}
    }
  
  	public boolean isNumeric(String value){
  		try{
  			Integer.parseInt(value);
  			return true;
  		}catch(NumberFormatException exp){
  			return false;
  		}
  	}
}
