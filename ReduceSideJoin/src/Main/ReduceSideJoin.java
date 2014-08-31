package Main;

import java.io.IOException;

import Average.AverageMapper;
import Average.AverageReducer;
import CustomPartitioner.MyComparator;
import CustomPartitioner.MyPartitioner;
import CustomPartitionerTwo.UserComparator;
import CustomPartitionerTwo.UserPartitioner;
import MovieRatingJoin.MovieMapper;
import MovieRatingJoin.MovieReducer;
import MovieRatingJoin.RatingMapper;
import MovieRatingJoin.TextPair;
import UserRatingJoin.RatingMapperTwo;
import UserRatingJoin.UserMapper;
import UserRatingJoin.UserPair;
import UserRatingJoin.UserReducer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ReduceSideJoin {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Job reduce_job_one = new Job(conf,"Reduce-Side Joiner I");				
		
		reduce_job_one.setOutputKeyClass(TextPair.class);
		reduce_job_one.setOutputValueClass(Text.class);
		reduce_job_one.setJarByClass(ReduceSideJoin.class);
		
		MultipleInputs.addInputPath(reduce_job_one, new Path("/user/bxr121630/input/movies.dat"), TextInputFormat.class, MovieMapper.class);
		MultipleInputs.addInputPath(reduce_job_one, new Path("/user/bxr121630/input/ratings.dat"), TextInputFormat.class, RatingMapper.class);
		reduce_job_one.setPartitionerClass(MyPartitioner.class);
		reduce_job_one.setGroupingComparatorClass(MyComparator.class);
		reduce_job_one.setReducerClass(MovieReducer.class);
		
		FileOutputFormat.setOutputPath(reduce_job_one, new Path("/user/bxr121630/temp-reduce1"));
		reduce_job_one.setOutputFormatClass(TextOutputFormat.class);
		if(reduce_job_one.waitForCompletion(true)) {
			Job reduce_job_two = new Job(conf,"Reduce-Side Joiner II");
			
			reduce_job_two.setOutputKeyClass(UserPair.class);
			reduce_job_two.setOutputValueClass(Text.class);
			reduce_job_two.setJarByClass(ReduceSideJoin.class);
			
			MultipleInputs.addInputPath(reduce_job_two, new Path("/user/bxr121630/input/users.dat"), TextInputFormat.class, UserMapper.class);
			MultipleInputs.addInputPath(reduce_job_two, new Path("/user/bxr121630/temp-reduce1/part-r-00000"), TextInputFormat.class, RatingMapperTwo.class);
			
			reduce_job_two.setPartitionerClass(UserPartitioner.class);
			reduce_job_two.setGroupingComparatorClass(UserComparator.class);
			reduce_job_two.setReducerClass(UserReducer.class);
			
			FileOutputFormat.setOutputPath(reduce_job_two, new Path("/user/bxr121630/temp-reduce2"));
			reduce_job_two.setOutputFormatClass(TextOutputFormat.class);
			if(reduce_job_two.waitForCompletion(true)) {
				Job reduce_job_three = new Job(conf,"Average Finder");
				
				reduce_job_three.setOutputKeyClass(Text.class);
				reduce_job_three.setOutputValueClass(IntWritable.class);
				reduce_job_three.setJarByClass(ReduceSideJoin.class);
				
				reduce_job_three.setMapperClass(AverageMapper.class);
				reduce_job_three.setReducerClass(AverageReducer.class);
				
				FileInputFormat.addInputPath(reduce_job_three, new Path("/user/bxr121630/temp-reduce2/part-r-00000"));
				FileOutputFormat.setOutputPath(reduce_job_three, new Path(args[0]));
				
				reduce_job_three.setInputFormatClass(TextInputFormat.class);
				reduce_job_three.setOutputFormatClass(TextOutputFormat.class);
				
				reduce_job_three.waitForCompletion(true);
			}
		}
	}

}
