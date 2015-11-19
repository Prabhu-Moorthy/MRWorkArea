package inputformat.linenumber;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
protected void map(LongWritable key, Text value, Context context) 
		throws java.io.IOException ,InterruptedException {
	context.write(key, value);
}
}
