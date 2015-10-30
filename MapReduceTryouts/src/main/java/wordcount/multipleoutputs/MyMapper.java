package wordcount.multipleoutputs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	static IntWritable one = new IntWritable(1);
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] valStrArr = value.toString().split(" ");
		for(String val:valStrArr){
			context.write(new Text(val), one);
		}
	}
}