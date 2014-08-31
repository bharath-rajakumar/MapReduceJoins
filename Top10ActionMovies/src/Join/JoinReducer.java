package Join;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
	@Override
	protected void reduce(IntWritable key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		for(Text movie_name : values)
		{
			context.write(key, new Text(movie_name));
		}			
	}
}
