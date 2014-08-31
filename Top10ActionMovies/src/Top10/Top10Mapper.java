package Top10;

import java.io.IOException;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class Top10Mapper extends
		Mapper<LongWritable, Text, NullWritable, Movie> {
	//Use TreeSet to collect top 10 average rated action movie
	private TreeSet<Movie> top_10 = new TreeSet<Movie>();
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String file_line = value.toString();
		String[] first_split = file_line.split("\\::");
		String movie_id = first_split[0];
		String movie_name = first_split[1];
		Double avg_rating = Double.parseDouble(first_split[2]);
		
		if(top_10.size() == 10) {
			top_10.add(new Movie(movie_id,movie_name,avg_rating));
			top_10.remove(top_10.first());
		} else {
			top_10.add(new Movie(movie_id,movie_name,avg_rating));
		}
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		for (Movie m : top_10) {
			context.write(NullWritable.get(), new Movie(m));
		}
	}
}
