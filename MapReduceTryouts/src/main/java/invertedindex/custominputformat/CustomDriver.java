package invertedindex.custominputformat;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CustomDriver extends Configured implements Tool {

	//http://hadooptutorial.info/creating-custom-hadoop-writable-data-type/
	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance();
		job.setJarByClass(CustomDriver.class);
		
		Path outputPath =  new Path(args[1]);
		outputPath.getFileSystem(getConf()).delete(outputPath, true);
		
		job.setMapperClass(CustomMapper.class);
		job.setReducerClass(CustomReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, outputPath);
		
		job.setNumReduceTasks(5);
		
		job.setMapOutputKeyClass(WebLogWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		return job.waitForCompletion(true) ? 0 : 1;
		
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new CustomDriver(), args);
	}

}