package MovieRatingJoin;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class MovieReducer extends
Reducer<TextPair, Text, NullWritable, Text> {
	String movie_id_name;
	@Override
	public void reduce(TextPair key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		if(key.second.contains("M"))
		{
			Integer movie_id = key.getFirst();
			String movie_name="";
			for(Text a: values) {
				movie_name = a.toString();
			}
			movie_id_name = movie_id + ">" + movie_name;
		} else {
			for(Text a: values) {
				context.write(NullWritable.get(), new Text(movie_id_name+">"+a.toString()));
			}
			
		}		
	}
	
}

