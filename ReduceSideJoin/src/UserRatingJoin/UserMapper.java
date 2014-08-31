package UserRatingJoin;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class UserMapper extends Mapper <LongWritable, Text, UserPair, Text>{
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String tag ="U";
		String file_line = value.toString();
		String[] first_split = file_line.split("\\::");
		if(first_split.length == 5) {
			Integer user_id = Integer.parseInt(first_split[0]);
			String gender = first_split[1];
			if(gender.contains("M")) {
				//This mapper will emit K<1,M>,V<Empty String>
				context.write(new UserPair(user_id,tag), new Text(""));
			}
		}
	}

}
