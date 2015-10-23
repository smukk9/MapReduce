
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

public class WordCount extends Configured implements Tool {
  public static void main(String[] args) throws Exception {	  
    	int exitCode = ToolRunner.run(new Configuration(), new WordCount(),args);
    System.exit(exitCode);
  }

@Override
public int run(String[] args) throws Exception {
	if (args.length != 2) {
	      System.err.println("Usage: WordCount <input path> <output path>");
	      System.exit(-1);
	    }

//Initializing the map reduce job
	Job job1= new Job(getConf());
	job1.setJarByClass(WordCount.class);
	job1.setJobName("wordcount");
	FileInputFormat.addInputPath(job1, new Path(args[0]));
	Path tempout = new Path("temp");
	SequenceFileOutputFormat.setOutputPath(job1, tempout);
    job1.setOutputFormatClass(SequenceFileOutputFormat.class);


	job1.setMapperClass(WordCountMapper.class);
	job1.setReducerClass(WordCountReducer.class);
	job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(IntWritable.class);
	job1.setOutputKeyClass(Text.class);
	job1.setOutputValueClass(IntWritable.class);
job1.waitForCompletion(true);

Job job2 = new Job();
job2.setJarByClass(WordCount.class);
job2.setJobName("WordCount2");

//The input of job2 is the output of job 1
job2.setInputFormatClass(SequenceFileInputFormat.class);
SequenceFileInputFormat.addInputPath(job2, tempout);
FileOutputFormat.setOutputPath(job2, new Path(args[1]));
job2.setMapperClass(WordCountMap2.class);
job2.setReducerClass(wordCountRed2.class);
job2.setMapOutputKeyClass(IntWritable.class);
job2.setMapOutputValueClass(Text.class);
job2.setOutputKeyClass(Text.class);
job2.setOutputValueClass(IntWritable.class);
job2.waitForCompletion(true);
return(job2.waitForCompletion(true) ? 0 : 1);


}
}
