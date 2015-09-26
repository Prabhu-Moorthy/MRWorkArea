package stackoverflow.stack;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper2 extends Mapper<Text, Text, Text, IntWritable> {
	protected void map(Text key, Text value, Context context) 
			throws java.io.IOException ,InterruptedException {
		String[] line = value.toString().split(",");
		String mKey = line[0];
		IntWritable mVal = new IntWritable(new Integer(line[2]));
		context.write(new Text(mKey), mVal);
	}
}
