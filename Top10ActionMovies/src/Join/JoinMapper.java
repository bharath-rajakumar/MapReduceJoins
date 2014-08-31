package Join;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.HashMap;

public class JoinMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

	HashMap<Integer, String> movie_map;

	// Retrieve movies.dat file from distributed cache and read it line-by-line
	@Override
	public void setup(Context context) throws IOException {
		try {
			Path[] file_path = DistributedCache.getLocalCacheFiles(context
					.getConfiguration());
			BufferedReader br = new BufferedReader(new FileReader(
					file_path[0].toString()));
			movie_map = new HashMap<Integer, String>();
			String file_line;
			String[] movie_info;
			while ((file_line = br.readLine()) != null) {
				// Split each line of movies.dat file
				// [MovieId::Movie_Name::Movie_Genres]
				movie_info = file_line.split("\\::");
				Integer movie_id = Integer.parseInt(movie_info[0]);
				String movie_name = movie_info[1];
				String movie_genre = movie_info[2];
				if (movie_genre.contains("Action")) {
					movie_map.put(movie_id, movie_name + " " + movie_genre);
				}
			}
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// HashMap Key<MovieId> Value<MovieName,Ratings1,Ratings2,...,RatingsN>

	// Read ratings.dat file line-by-line
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String file_line = value.toString();
		String[] movie_rating_info = file_line.split("\\::");
		// Split each line of ratings.dat file
		// [UserId::MovieId::Movie_Rating::Date_Time]
		if (movie_rating_info.length == 4) {
			Integer movie_id = Integer.parseInt(movie_rating_info[1]);
			String movie_rating = movie_rating_info[2];
			if (movie_map.containsKey(movie_id)) {
				context.write(new IntWritable(movie_id), new Text("::"
						+ movie_map.get(movie_id) + "::" + movie_rating));
			}
		}

	}
}