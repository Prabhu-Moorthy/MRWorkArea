package stackoverflow.stack;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Driver extends Configured implements Tool{

	public int run(String[] args) throws Exception {
		//Job1

		//Initilization
		Job job1 = Job.getInstance();
		Path gameFile = new Path(args[0]);
		Path ageFile = new Path(args[1]);
		Path outputPath1 = new Path(args[2]);

		//Cleanup
		FileSystem.get(getConf()).delete(outputPath1, true);

		//Input and Output Formats
		MultipleInputs.addInputPath(job1, gameFile, TextInputFormat.class,GameMapper.class);
		MultipleInputs.addInputPath(job1, ageFile, TextInputFormat.class,AgeMapper.class);
		FileOutputFormat.setOutputPath(job1, outputPath1);

		//Job Run
		job1.setJarByClass(Driver.class);	

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);

		job1.setReducerClass(Reducer1.class);

		job1.waitForCompletion(true);

		//Job2

		Job job2 = Job.getInstance();

		Path inputPath2 = outputPath1;
		Path outputPath2 = new Path(args[3]);

		//Cleanup
		FileSystem.get(getConf()).delete(outputPath2, true);

		//Input and Output Formats
		job2.setInputFormatClass(KeyValueTextInputFormat.class);
		KeyValueTextInputFormat.addInputPath(job2, inputPath2);
		TextOutputFormat.setOutputPath(job2, outputPath2);

		//Job Run
		job2.setJarByClass(Driver.class);
		job2.setMapperClass(Mapper2.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(IntWritable.class);

		job2.setNumReduceTasks(1);
		job2.setReducerClass(Reducer2.class);

		return job2.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Driver(), args);
	}

}