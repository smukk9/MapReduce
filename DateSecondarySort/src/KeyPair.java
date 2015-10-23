import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.*;


public class KeyPair implements WritableComparable<KeyPair>{
 
	//the key pair holds the lastname and birthyear
	private Text lastname;
	private IntWritable birthyear;
	
	//The defaule constructor
	public KeyPair()
	{
		lastname = new Text();
		birthyear= new IntWritable();
	}
	
	//constructor, initializing the lastname and birthyear
	public KeyPair(String last, int year)
	{
		lastname = new Text(last);
		birthyear= new IntWritable(year);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		lastname.readFields(in);
		birthyear.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		lastname.write(out);
		birthyear.write(out);
	}

	@Override
	public int compareTo(KeyPair otherPair) {
		// TODO Auto-generated method stub
		int c= lastname.compareTo(otherPair.lastname);
		if (c!=0)
			return c;
		else
			return birthyear.compareTo(otherPair.birthyear);
	}
	
	//the Getter and setter methods
	public Text getLastname() {
		return lastname;
	}

	public void setLastname(Text lastname) {
		this.lastname = lastname;
	}

	public IntWritable getBirthyear() {
		return birthyear;
	}

	public void setBirthyear(IntWritable birthyear) {
		this.birthyear = birthyear;
	}

}
