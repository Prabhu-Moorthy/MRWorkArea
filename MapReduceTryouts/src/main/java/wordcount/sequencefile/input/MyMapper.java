package wordcount.sequencefile.input;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<Text, IntWritable, NullWritable, Text> {
	static IntWritable ONE = new IntWritable(1);
	@Override
	protected void map(Text key, IntWritable value,Context context)
			throws IOException, InterruptedException {
		Text newValue = new Text();
		String concatenatedVal = value.toString().concat("\t").concat(key.toString());
		System.err.println(concatenatedVal);
		NullWritable nullKey = NullWritable.get();
		context.write(nullKey, newValue);
	}
}