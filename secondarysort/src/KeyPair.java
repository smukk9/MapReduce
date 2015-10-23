import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.*;


public class KeyPair implements WritableComparable<KeyPair>{
 
	//the key pair holds the ArivalDelay and Flightnumber
	private IntWritable ArivalDelay;
	private IntWritable Flightnumber;
	
	//The defaule constructor
	public KeyPair()
	{
		ArivalDelay = new IntWritable();
		Flightnumber= new IntWritable();
	}
	
	//constructor, initializing the ArivalDelay and Flightnumber
	public KeyPair(int delay, int year)
	{
		ArivalDelay = new IntWritable(delay);
		Flightnumber= new IntWritable(year);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		ArivalDelay.readFields(in);
		Flightnumber.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		ArivalDelay.write(out);
		Flightnumber.write(out);
	}

	@Override
	public int compareTo(KeyPair otherPair) {
		// TODO Auto-generated method stub
		int c= ArivalDelay.compareTo(otherPair.ArivalDelay);
		if (c!=0)
			return c;
		else
			return Flightnumber.compareTo(otherPair.Flightnumber);
	}
	
	//the Getter and setter methods
	public IntWritable getArivalDelay() {
		return ArivalDelay;
	}

	public void setArivalDelay(IntWritable ArivalDelay) {
		this.ArivalDelay = ArivalDelay;
	}

	public IntWritable getFlightnumber() {
		return Flightnumber;
	}

	public void setFlightnumber(IntWritable Flightnumber) {
		this.Flightnumber = Flightnumber;
	}

}
