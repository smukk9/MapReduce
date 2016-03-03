import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.*;
/**
 * A comparator for the KeyPair class that compairs by lastname and then birth year Desc
 * This comparator is used for sorting the keypair by the mapreduce framework
 * @author Ellie Buxton
 *
 */
public class IPTimeComparator extends WritableComparator {
	
	public IPTimeComparator() {
		super(KeyPair.class, true);	
	}
	
	public int compare(WritableComparable k1, WritableComparable k2)
	{
		
		
		KeyPair key1 = (KeyPair) k1;
		KeyPair key2= (KeyPair) k2;
		int c = key1.getIP().compareTo(key2.getIP());
		if(c == 0){
	String	timeaccessed1 = key1.getAccessTIme().toString();
	String	timeaccessed2 = key2.getAccessTIme().toString();
	try{
		DateFormat df1 = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss z");
		
		Date d1 = df1.parse(timeaccessed1);
		Date d2 = df1.parse(timeaccessed2);
		
			if(d1.before(d2)){
				
				return 1;
			}else if(d1.after(d2)){
				return -1;
			}else
				return 0;
			
		}catch(Exception e){
		e.printStackTrace();
		return -1;
		
	}
	}else{
		return c;
	}
	
}
}