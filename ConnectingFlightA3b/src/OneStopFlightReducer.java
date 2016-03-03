import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class OneStopFlightReducer extends Reducer<Text, TuplePair, NullWritable, Text> {

  public void reduce(Text joinfield, Iterable<TuplePair> values, Context context)
      throws IOException, InterruptedException {

    //An array to store the tuples coming from the A relation
	  ArrayList<String> ARecords = new ArrayList<String>();
	  
	//an array to store the tuples coming from the B relation
	  ArrayList<String> BRecords = new ArrayList<String>();
	 
   
    //Separating the tuples associated with A and B into ARecords and BRecords
    for (TuplePair tp : values) {

      if (tp.getRelationName().equals("A"))
        ARecords.add(tp.getTuple());
      else
        BRecords.add(tp.getTuple());


    }
    
   
    for (String record1 : ARecords ){
   	 for (String record2 : BRecords){
   		 
   		 String [] Avalues = record1.toString().split(",");
   		 String [] Bvalues = record2.toString().split(",");	 
   		 String Bdepttime = Bvalues[1];
   		 String Aarivaltime = Avalues[0];
   		 String Barivaltime = Bvalues[0];
   		 int atime = Integer.parseInt(Aarivaltime);
   	    int bTime = Integer.parseInt(Barivaltime);
   	    int mins = atime % 100;
   	    int hrs = atime/100;
   	    String AExacttime = String.valueOf(hrs)+":"+String.valueOf(mins);
   	    int bmins = bTime % 100;
   	    int bhrs = bTime/100;
   	    String BExacttime = String.valueOf(bhrs)+":"+String.valueOf(bmins);
   	    try{
   		 DateFormat df = new SimpleDateFormat("hh:mm");
   		 Date adate = df.parse(AExacttime) ;
   		 Date bdate = df.parse(BExacttime);
   		long diff = adate.getTime() - bdate.getTime();
  		long DiffSEC = -1* (diff / 1000);
   		 if(DiffSEC > 3600 && DiffSEC <=18000 ){
  				
    String outValue = Avalues[2]+" "+Bvalues[2]+" "+Avalues[3]+" "+Avalues[4]+" " +Bvalues[4]+" "+Bvalues[3	]+" "
  						  + Aarivaltime+" "+ Bdepttime;
    context.write(NullWritable.get(), new Text(outValue));
    }
  			   }
   	 catch(Exception e){
   		 e.printStackTrace();
   	 }
  	   }
   	 }
  }
    }
    
    
 
    
      