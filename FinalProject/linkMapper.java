//Author: sandeepyadav mukkamala
//mapper program to extract all the index and their respestive refferences
import java.io.IOException;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

//
public class linkMapper extends
Mapper<LongWritable, Text,IntWritable ,IntWritable> {
public void map(LongWritable key, Text value, Context context)
  throws IOException, InterruptedException {
	
	 
	
String values = value.toString().trim();
String data = values.substring(2, values.length()-2);
String [] records = data.toString().split("#index|#%");

try{
String index = records[1].replaceAll("\\D+","");

int paperIndex = Integer.parseInt(index);

if((records.length >=2)){
	
	for(int i=2;i<records.length;i++)
	{
	
	
		
	 String citedPaper  = records[i].trim();
	 
	 if(citedPaper.matches("[0-9]+")){
		 int ref =Integer.parseInt(citedPaper);
		 context.write(new IntWritable(paperIndex), new IntWritable(ref));
	 }

	

 
	
}
}
 

}catch(Exception e)
{
	int h=0;
	for(String a: records){
		h++;
		System.out.println(h +a);
		//System.out.println(paperIndex);

	}
}
}

}


