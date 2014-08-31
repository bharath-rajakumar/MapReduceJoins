package CustomPartitionerTwo;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import UserRatingJoin.UserPair;;

public class UserPartitioner extends Partitioner<UserPair, Text> {

	@Override
	public int getPartition(UserPair key, Text value, int numPartitions) {
		// TODO Auto-generated method stub
		return (key.gethashcode() % numPartitions);
	}
	
}