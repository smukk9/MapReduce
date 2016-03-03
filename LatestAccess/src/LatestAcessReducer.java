import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.util.*;


public class LatestAcessReducer extends Reducer<KeyPair, Text, Text,Text >{
	public void reduce(KeyPair key, Iterable<Text> values, Context context)
			  throws IOException, InterruptedException {
		
		Iterator<Text> it= values.iterator();
		if(it.hasNext())
			context.write(key.getIP(), new Text ("[" + values.iterator().next()+"]"));
	}
}
