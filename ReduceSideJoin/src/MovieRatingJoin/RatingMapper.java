package MovieRatingJoin;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class RatingMapper extends Mapper<LongWritable, Text, TextPair, Text> {
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String tag = "R";
		String file_line = value.toString();
		String[] first_split = file_line.split("\\::");
		if (first_split.length == 4) {
			String user_id = first_split[0];
			Integer movie_id = Integer.parseInt(first_split[1]);
			String movie_rating = first_split[2];
			// This mapper will emit K<1,1>,V<948 3>
			context.write(new TextPair(movie_id, tag), new Text(user_id + ">"
					+ movie_rating));
		}
	}
}
