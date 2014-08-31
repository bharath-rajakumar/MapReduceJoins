package Average;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AverageMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String file_line = value.toString();
		String[] first_split = file_line.split(">");
		if(!(first_split[0].contains("null"))) {
			if(first_split.length == 4) {
				String movie_id = first_split[1];
				String movie_name = first_split[2];
				Integer rating = Integer.parseInt(first_split[3]);
				context.write(new Text(movie_id+">"+movie_name+">"),
						new IntWritable(rating));
			}
		}
		
		
	}

}