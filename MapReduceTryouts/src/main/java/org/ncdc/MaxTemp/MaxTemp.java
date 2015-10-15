package org.ncdc.MaxTemp;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTemp {

	public static void main(String... args) throws Exception {
		if(args.length != 2){
			System.err.print("Usage : MaxTemp <input path> <output path>");
		}
		
		Job job = new Job();
		job.setJarByClass(MaxTemp.class);
		job.setJobName("Max Temp");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(ClimateMapper.class);
		job.setReducerClass(ClimateReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

