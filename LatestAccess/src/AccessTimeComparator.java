import org.apache.hadoop.io.*;

/**
 * A comparator for the the Key-Pair class. This comparator compares only by lastname.
 * This comparator is used for grouping the key pairs.
 * @author Ellie Buxton
 *
 */
public class AccessTimeComparator extends WritableComparator{
	
	public AccessTimeComparator() {
		super(KeyPair.class, true);
		
	}
	public int compare(WritableComparable k1, WritableComparable k2)
	{
		KeyPair key1 = (KeyPair) k1;
		KeyPair key2= (KeyPair) k2;
		return key1.getIP().compareTo(key2.getIP());
	}
}
