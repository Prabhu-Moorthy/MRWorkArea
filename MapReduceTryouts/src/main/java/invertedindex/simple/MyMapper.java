package invertedindex.simple;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
	enum customCounters{
		POSITIVE
	}
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		FileSplit split = (FileSplit)context.getInputSplit();
		String fileName = split.getPath().getName();
		String[] line = value.toString().split("\\s");
		for(String val:line){
			context.write(new Text(val), new Text(fileName));
			context.getCounter(customCounters.POSITIVE).increment(1);
		}
	}
}