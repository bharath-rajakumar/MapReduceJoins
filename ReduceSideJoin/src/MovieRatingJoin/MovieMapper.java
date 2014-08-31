package MovieRatingJoin;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieMapper extends Mapper<LongWritable, Text, TextPair, Text> {
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String tag = "M";
		String file_line = value.toString();
		String[] first_split = file_line.split("\\::");
		if (first_split.length == 3) {
			Integer movie_id = Integer.parseInt(first_split[0]);
			String movie_name = first_split[1];
			String movie_genre = first_split[2];
			TextPair pair = new TextPair(movie_id, tag);
			if (movie_genre.contains("Action") || movie_genre.contains("Drama")) {
				// This mapper will emit K<1,0>,V<Toy Story
				// Animation|Children's|Comedy>
				context.write(new TextPair(pair), new Text(movie_name + " "
						+ movie_genre));
			}
		}
	}
}
