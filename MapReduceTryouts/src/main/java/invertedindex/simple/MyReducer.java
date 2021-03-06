package invertedindex.simple;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, Text, Text, Text> {
	enum customCounters{
		POSITIVE
	}
	@Override
	protected void reduce(Text key, Iterable<Text> values,Context context)
			throws IOException, InterruptedException {
		Set<Text> optSet = new TreeSet<>();
		for(Text value:values){
			Text newVal = new Text(value);
			optSet.add(newVal);
			context.getCounter(customCounters.POSITIVE).increment(-1);
		}
		context.write(key, new Text(optSet.toString()));
	}
}
