package Average;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageReducer extends
		Reducer<Text, IntWritable, Text, DoubleWritable> {
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		int sum_of_ratings = 0;
		int count_of_ratings = 0;	
		Double average_rating = 0.0;
		for(IntWritable value : values) {
			sum_of_ratings = sum_of_ratings + value.get();
			count_of_ratings = count_of_ratings + 1;			
		}
		average_rating = ((sum_of_ratings * 1.0) /count_of_ratings);
		if((average_rating >= 4.4) && (average_rating <= 4.7)) {
			context.write(new Text(key), new DoubleWritable(average_rating));
		}
		
	}
}
