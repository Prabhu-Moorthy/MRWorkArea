package inputformat.linenumber;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MyDriver extends Configured implements Tool{

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf());
		job.setJarByClass(MyDriver.class);
		Path outputPath = new Path(args[1]);
		outputPath.getFileSystem(getConf()).delete(outputPath,true);
		LineNumberInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, outputPath);
		job.setMapperClass(MyMapper.class);
		job.setInputFormatClass(LineNumberInputFormat.class);
		job.setNumReduceTasks(0);
		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new MyDriver(), args);
	}
}
