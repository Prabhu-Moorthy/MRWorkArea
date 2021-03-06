package invertedindex.distributedcache;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * hadoop jar MapReduceTryouts-1.jar invertedindex.distributedcache.MyDriver -D mapreduce.job.reduces=1  /user/notprabhu2/Input/sample/my* NewOpt
 * @author Prabhu
 *
 */
public class MyDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf);
		job.setJarByClass(MyDriver.class);

		Path outputPath =  new Path(args[1]);
		outputPath.getFileSystem(getConf()).delete(outputPath, true);
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		DistributedCache.addCacheFile(new URI("/user/notprabhu2/Input/sample/stopwords.txt"),job.getConfiguration());
		
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, outputPath);
		
		//job.setNumReduceTasks(5);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		return job.waitForCompletion(true) ? 0 : 1;
		
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(),new MyDriver(), args);
	}

}