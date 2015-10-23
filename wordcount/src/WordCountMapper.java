import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class WordCountMapper extends
    Mapper<LongWritable, Text, Text, IntWritable> {
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
   ///Replacing all digits and punctuation with an empty string
	  String line = value.toString().replaceAll("\\p{Punct}|\\d", "").toLowerCase();
   //Extracting the words
	  StringTokenizer record = new StringTokenizer(line);
   //Emitting each word as a key and one as its value
	  while (record.hasMoreTokens())
     context.write(new Text(record.nextToken()), new IntWritable(1));
  
    }
  }

