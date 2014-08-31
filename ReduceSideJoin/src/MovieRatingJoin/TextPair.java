package MovieRatingJoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class TextPair implements WritableComparable<TextPair>,Writable{

	Integer first;
	String second;
	
	public TextPair()
	{
		this.first = 0;
		this.second = "";
	}
	
	public TextPair(Integer one, String two) {
		this.first = Integer.parseInt(one.toString());
		this.second = two.toString();
	}
	
	public TextPair(TextPair textpair) {
		this.first = textpair.first;
		this.second = textpair.second;
	}
	
	public int getFirst() {
		return this.first;
	}
	
	public String getSecond() {
		return this.second;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.first = in.readInt();
		this.second = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(first);
		out.writeUTF(second);
	}

	@Override
	public int compareTo(TextPair textpair) {
		// TODO Auto-generated method stub
		if(this.first > textpair.first) {
			return 1;
		} else if(this.first == textpair.first){
			return (this.second.compareTo(textpair.second));
		} 
		return -1;
	}
	
	public int gethashcode() {
		return this.first.hashCode();
	}
	
	public boolean equals(TextPair textpair) {
		if(this.first == textpair.first) {
			return true;
		} else {
			return false;
		}
	}
	
	public String toString() {
		return this.first + ">" +this.second;
	}

}
