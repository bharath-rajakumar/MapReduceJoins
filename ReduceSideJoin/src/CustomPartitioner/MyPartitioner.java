package CustomPartitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import MovieRatingJoin.TextPair;

public class MyPartitioner extends Partitioner<TextPair, Text> {

	@Override
	public int getPartition(TextPair key, Text value, int numPartitions) {
		// TODO Auto-generated method stub
		return (key.gethashcode() % numPartitions);
	}
	
}