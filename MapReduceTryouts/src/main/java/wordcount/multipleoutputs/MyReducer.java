package wordcount.multipleoutputs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	MultipleOutputs mos;
	
	public void setup(Context context) throws IOException ,InterruptedException {
		mos = new MultipleOutputs<>(context);
		
	}
	
	protected void reduce(Text key, Iterable<IntWritable> value,Context context) throws IOException, InterruptedException {
		int sum = 0;
		for(IntWritable val:value){
			sum++;
		}
		mos.write("text",key, new IntWritable(sum),"/user/notprabhu2/optMulti/multText");
		mos.write("seq" ,key, new IntWritable(sum),"/user/notprabhu2/optMulti/seqText");
	}
}