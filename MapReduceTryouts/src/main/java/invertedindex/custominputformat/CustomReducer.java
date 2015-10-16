package invertedindex.custominputformat;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CustomReducer extends Reducer<WebLogWritable, IntWritable, Text, IntWritable> {
	private IntWritable result = new IntWritable();
	private Text ip = new Text();

	protected void reduce(WebLogWritable key, Iterable<IntWritable> values,Context context)
			throws IOException, InterruptedException {
		int sum = 0;
	    ip = key.getIp(); 
	    
	    for (IntWritable wLog : values) 
	    {
	      sum++ ;
	    }
	    result.set(sum);
	    context.write(ip, result);
	}
}