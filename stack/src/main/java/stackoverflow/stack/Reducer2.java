package stackoverflow.stack;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer2 extends Reducer<Text, IntWritable, Text, IntWritable> {
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) 
			throws java.io.IOException ,InterruptedException {
		int sum = 0;
		int count = 0;
		int avg = 0;
		for(IntWritable rVal:values){
			sum = sum + rVal.get();
			count++;
		}
		avg = sum/count;
		context.write(key,new IntWritable(avg));
	}
}
