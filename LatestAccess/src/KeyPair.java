import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.*;


public class KeyPair implements WritableComparable<KeyPair>{
 
	//the key pair holds the IP and AccessTIme
	private Text IP;
	private Text AccessTIme;
	
	//The default constructor
	public KeyPair()
	{
		IP = new Text();
		AccessTIme= new Text();
	}
	
	//constructor, initializing the IP and AccessTIme
	public KeyPair(String iP2, String accessTime2)
	{
		IP = new Text(iP2);
		AccessTIme= new Text(accessTime2);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		IP.readFields(in);
		AccessTIme.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		IP.write(out);
		AccessTIme.write(out);
	}

	@Override
	public int compareTo(KeyPair otherPair) {
		// TODO Auto-generated method stub
		int c= IP.compareTo(otherPair.IP);
		if (c!=0)
			return c;
		else
			return AccessTIme.compareTo(otherPair.AccessTIme);
	}
	
	//the Getter and setter methods
	public Text getIP() {
		return IP;
	}

	public void setIP(Text IP) {
		this.IP = IP;
	}

	public Text getAccessTIme() {
		return AccessTIme;
	}

	public void setAccessTIme(Text AccessTIme) {
		this.AccessTIme = AccessTIme;
	}

}
