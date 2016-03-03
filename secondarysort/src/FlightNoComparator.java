import org.apache.hadoop.io.*;

/**
 * A comparator for the the Key-Pair class. This comparator compares only by lastname.
 * This comparator is used for grouping the key pairs.
 * @author Ellie Buxton
 *
 */
public class FlightNoComparator extends WritableComparator{
	
	public FlightNoComparator() {
		super(KeyPair.class, true);
		
	}
	public int compare(WritableComparable k1, WritableComparable k2)
	{
		KeyPair key1 = (KeyPair) k1;
		KeyPair key2= (KeyPair) k2;
		return key1.getFlightnumber().compareTo(key2.getFlightnumber());
	}
}
