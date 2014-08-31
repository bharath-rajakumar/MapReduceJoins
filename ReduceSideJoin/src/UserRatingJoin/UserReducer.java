package UserRatingJoin;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer; 

import UserRatingJoin.UserPair;

public class UserReducer extends Reducer<UserPair, Text, NullWritable, Text> {
	Integer user_id_name;
	
	@Override
	public void reduce(UserPair key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		if(key.tag.contains("U")) {
			user_id_name = key.getUserId();
		} else {
			for(Text a : values) {
				context.write(NullWritable.get(), new Text(user_id_name+">"+a.toString()));
			}
		}
	}
}
