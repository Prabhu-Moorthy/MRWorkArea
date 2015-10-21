package wordcount.sequencefile.input;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
	enum customCounters{
		POSITIVE
	}
	@Override
	protected void reduce(NullWritable key, Iterable<Text> values,Context context)
			throws IOException, InterruptedException {
		for(Text value:values){
			context.write(key, value);
		}
		
	}
}
