import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

/**
 * A custom partitioner class that partitions the keys only by lastname
 * This ensures that all keys with the same lastname go to the same reducer
 * @author elhams
 *
 */
public class NamePartitioner extends Partitioner<KeyPair, IntWritable>{

	@Override
	public int getPartition(KeyPair key, IntWritable value, int numReducers) {
		// TODO Auto-generated method stub
		return key.getLastname().hashCode()% numReducers;
	
	}
	

}
