import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

/**
 * A custom partitioner class that partitions the keys only by lastname
 * This ensures that all keys with the same lastname go to the same reducer
 * @author elhams
 *
 */
public class FlighNoPartitioner extends Partitioner<KeyPair, Text>{

	@Override
	public int getPartition(KeyPair key, Text value, int numReducers) {
		// TODO Auto-generated method stub
		return key.getFlightNo().hashCode()% numReducers;
	
	}
	

}
