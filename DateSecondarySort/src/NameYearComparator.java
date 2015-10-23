import org.apache.hadoop.io.*;
/**
 * A comparator for the KeyPair class that compairs by lastname and then birth year Desc
 * This comparator is used for sorting the keypair by the mapreduce framework
 * @author Ellie Buxton
 *
 */
public class NameYearComparator extends WritableComparator {
	
	public NameYearComparator() {
		super(KeyPair.class, true);	
	}
	
	public int compare(WritableComparable k1, WritableComparable k2)
	{
		KeyPair key1 = (KeyPair) k1;
		KeyPair key2= (KeyPair) k2;
		int c = key1.getLastname().compareTo(key2.getLastname());
		if (c ==0)
			return -key1.getBirthyear().compareTo(key2.getBirthyear());
		else
			return c;
	}
}
