package Top10;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.TreeSet;
import java.util.ArrayList;
import java.util.Collections;

public class Top10Reducer extends
		Reducer<NullWritable, Movie, NullWritable, Text> {

	public void reduce(NullWritable key, Iterable<Movie> values, Context context)
			throws IOException, InterruptedException {
		TreeSet<Movie> top_10 = new TreeSet<Movie>();
		for(Movie m : values) {
			if(top_10.size() == 10) {
				top_10.add(new Movie(m));
				top_10.remove(top_10.first());
			} else {
				top_10.add(new Movie(m));
			}
		}
		
		ArrayList<Movie> descending_order = new ArrayList<Movie>();
		descending_order.addAll(top_10);
		Collections.reverse(descending_order);
		
		for(Movie m : descending_order) {
			context.write(NullWritable.get(), new Text(m.toString()));
		}
	}
}
