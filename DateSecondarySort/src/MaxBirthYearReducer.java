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
public class MaxBirthYearReducer extends Reducer<KeyPair, IntWritable, Text, IntWritable>{
	public void reduce(KeyPair key, Iterable<IntWritable> values, Context context)
			  throws IOException, InterruptedException {
		
		Iterator<IntWritable> it= values.iterator();
		if(it.hasNext())
			context.write(key.getLastname(), values.iterator().next());
	}
}
