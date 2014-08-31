package UserRatingJoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class UserPair implements WritableComparable<UserPair>, Writable{

	Integer user_id;
	String tag;
	
	public UserPair() {
		this.user_id = 0;
		this.tag = "";
	}
	
	public UserPair(Integer u, String t) {
		this.user_id = u;
		this.tag = t;
	}
	
	public UserPair(UserPair userpair) {
		this.user_id = userpair.user_id;
		this.tag = userpair.tag;
	}
	
	public int getUserId() {
		return this.user_id;
	}
	
	public String getTag() {
		return this.tag;
	}
	
	
	
	@Override
	public int compareTo(UserPair u) {
		// TODO Auto-generated method stub
		if(this.user_id > u.user_id) {
			return 1;
		} else if(this.user_id == u.user_id){
			return (this.tag.compareTo(u.tag));
		} 
		return -1;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.user_id = in.readInt();
		this.tag = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(user_id);
		out.writeUTF(tag);
	}
	
	public int gethashcode() {
		return this.user_id.hashCode();
	}
	
	public boolean equals(UserPair u) {
		if(this.user_id == u.user_id) {
			return true;
		} else {
			return false;
		}
	}
	
	public String toString() {
		return this.user_id + " " +this.tag;
	}

}
