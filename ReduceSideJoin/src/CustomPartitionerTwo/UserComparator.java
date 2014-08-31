package CustomPartitionerTwo;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import UserRatingJoin.UserPair;

public class UserComparator extends WritableComparator {

	public UserComparator() {
		super(UserPair.class, true);
	}
	
	@Override
	public int compare(WritableComparable a, WritableComparable b ) {
		UserPair one = (UserPair)a;
		UserPair two = (UserPair)b;
		return one.compareTo(two);
	}
}
