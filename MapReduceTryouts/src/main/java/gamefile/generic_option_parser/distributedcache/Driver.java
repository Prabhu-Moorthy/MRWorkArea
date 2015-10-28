package gamefile.generic_option_parser.distributedcache;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
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
		Configuration conf = getConf();
		

		//Initilization
		Job job1 = Job.getInstance(conf);
		Path gameFile = new Path(args[0]);
		Path outputPath1 = new Path(args[1]);

		//Cleanup
		FileSystem.get(getConf()).delete(outputPath1, true);

		//Input and Output Formats
		TextInputFormat.addInputPath(job1, gameFile);
		TextOutputFormat.setOutputPath(job1, outputPath1);

		//Job Run
		job1.setJarByClass(Driver.class);
		
		job1.setMapperClass(GameMapper.class);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);

		//job1.setReducerClass(Reducer1.class);

		return job1.waitForCompletion(true) ? 0 : 1;

	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(),new Driver(), args);
	}

}