import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.*;


public class KeyPair implements WritableComparable<KeyPair>{
 
	//the key pair holds the lastname and birthyear
	private IntWritable flightNo;
	private IntWritable arrivalDelay;
	
	//The defaule constructor
	public KeyPair()
	{
		flightNo = new IntWritable();
		arrivalDelay= new IntWritable();
	}
	
	//constructor, initializing the lastname and birthyear
	public KeyPair(int last, int year)
	{
		flightNo = new IntWritable(last);
		arrivalDelay= new IntWritable(year);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		flightNo.readFields(in);
		arrivalDelay.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		flightNo.write(out);
		arrivalDelay.write(out);
	}

	@Override
	public int compareTo(KeyPair otherPair) {
		// TODO Auto-generated method stub
		int c= flightNo.compareTo(otherPair.flightNo);
		if (c!=0)
			return c;
		else
			return arrivalDelay.compareTo(otherPair.arrivalDelay);
	}

	public IntWritable getFlightNo() {
		return flightNo;
	}

	public void setFlightNo(IntWritable flightNo) {
		this.flightNo = flightNo;
	}

	public IntWritable getArrivalDelay() {
		return arrivalDelay;
	}

	public void setArrivalDelay(IntWritable arrivalDelay) {
		this.arrivalDelay = arrivalDelay;
	}
	
	//the Getter and setter methods
	

}
