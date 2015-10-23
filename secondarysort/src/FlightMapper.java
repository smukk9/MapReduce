import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlightMapper 
	extends Mapper<LongWritable, Text , KeyPair, Text> {
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
   
   //Extracting the fields
	  String[] record = value.toString().split(",");
	  
   /* 
    * Emitting The Flightnumber and birth year from the record
    * the records are in this format: Flightnumber  firstname Flightnumber-month-day , e.g., "Jones Zeke 2001-DEC-12"
    * The mapper emits a pair of Flightnumber and birth year as key and the birth year as value, e.g. key= Keypair(Jones,2001), value=2001  
   */ 
	 try{
		 if(record.length > 15)
		 {
		 String flighdelay = record[14];
		 String Flightnum = record[9];
		 int ArivalDelay = Integer.parseInt(flighdelay);
		 int Flightnumber = Integer.parseInt(Flightnum);
		
		 
					 
		 context.write(new KeyPair(Flightnumber, ArivalDelay), new Text(value));
	 }
	 }catch(Exception e){
		 e.printStackTrace();
	 }
	 }
    }

