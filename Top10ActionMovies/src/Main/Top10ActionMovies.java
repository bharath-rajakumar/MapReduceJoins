package Main;
import Join.JoinMapper;
import Join.JoinReducer;
import Average.AverageMapper;
import Average.AverageReducer;
import Top10.Top10Mapper;
import Top10.Top10Reducer;
import Top10.Movie;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Top10ActionMovies {
	
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		//Adding the smaller file to distributed cache
		DistributedCache.addCacheFile(new Path("/user/bxr121630/input/movies.dat").toUri(), conf);
		Job joiner_job = new Job(conf,"Map-Side Joiner");				
		
		joiner_job.setOutputKeyClass(IntWritable.class);
		joiner_job.setOutputValueClass(Text.class);
		
		joiner_job.setJarByClass(Top10ActionMovies.class);
		
		joiner_job.setMapperClass(JoinMapper.class);
		joiner_job.setReducerClass(JoinReducer.class);
		
		joiner_job.setInputFormatClass(TextInputFormat.class);
		joiner_job.setOutputFormatClass(TextOutputFormat.class);
		
	
		String join_output = "/user/bxr121630/temp-join";
		FileInputFormat.addInputPath(joiner_job,new Path(args[0]));
		FileOutputFormat.setOutputPath(joiner_job,new Path(join_output));
		
		if(joiner_job.waitForCompletion(true)) {
			Job average_job = new Job(conf, "Average Ratings");
			average_job.setOutputKeyClass(Text.class);
			average_job.setOutputValueClass(IntWritable.class);
			
			average_job.setJarByClass(Top10ActionMovies.class);
			average_job.setMapperClass(AverageMapper.class);
			average_job.setReducerClass(AverageReducer.class);
			
			average_job.setInputFormatClass(TextInputFormat.class);
			average_job.setOutputFormatClass(TextOutputFormat.class);
			
			String average_output = "/user/bxr121630/temp-average";
			FileInputFormat.addInputPath(average_job,new Path(join_output));
			FileOutputFormat.setOutputPath(average_job, new Path(average_output));
			
			if(average_job.waitForCompletion(true)) {
				Job top10_job = new Job(conf, "Top 10 Rated Action Movie");
				top10_job.setOutputKeyClass(NullWritable.class);
				top10_job.setOutputValueClass(Movie.class);
				
				top10_job.setJarByClass(Top10ActionMovies.class);
				top10_job.setMapperClass(Top10Mapper.class);
				top10_job.setReducerClass(Top10Reducer.class);
				
				top10_job.setInputFormatClass(TextInputFormat.class);
				top10_job.setOutputFormatClass(TextOutputFormat.class);
				
				FileInputFormat.addInputPath(top10_job, new Path(average_output));
				FileOutputFormat.setOutputPath(top10_job, new Path(args[1]));
				
				top10_job.waitForCompletion(true);
			}
		}

	}

}
