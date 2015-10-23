import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import java.util.*;

/**
 * The reducer for the MaxBirthYear problem. 
 * The reducer receives a last name as a key and all the birth year associated with it in a DESC order
 * The reducer simply emits the lastname and the first birth year (max birth year) in the values
 * @author elhams
 *
 */
public class FlightReducer extends Reducer<KeyPair, Text, NullWritable, Text>{
	public void reduce(KeyPair key, Iterable<Text> values, Context context)
			  throws IOException, InterruptedException {
		
		Iterator<Text> it= values.iterator();
		while(it.hasNext())
			context.write(NullWritable.get(), values.iterator().next());
	}
}
