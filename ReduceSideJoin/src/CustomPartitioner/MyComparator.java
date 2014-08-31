package CustomPartitioner;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import MovieRatingJoin.TextPair;

public class MyComparator extends WritableComparator {

	public MyComparator() {
		super(TextPair.class, true);
	}
	
	@Override
	public int compare(WritableComparable a, WritableComparable b ) {
		TextPair one = (TextPair)a;
		TextPair two = (TextPair)b;
		return one.compareTo(two);
	}
}
