package UserRatingJoin;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class RatingMapperTwo extends Mapper<LongWritable, Text, UserPair, Text>{
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String tag = "J";
		String file_line = value.toString();
		String[] first_split = file_line.split(">");
		if(!(first_split[0].contains("null")))
		{
			System.out.println("error"+" "+file_line);
			Integer user_id = Integer.parseInt(first_split[2]);
			String movie_id = first_split[0];
			String movie_name = first_split[1];
			String rating = first_split[3];
			context.write(new UserPair(user_id,tag), new Text(movie_id+">"+movie_name+">"+rating));
		}
		
	}

}
