package stackoverflow.stack;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class AgeMapper extends Mapper<LongWritable, Text, Text, Text> {

	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		String[] line = value.toString().split(",");
		String mKeyStr = line[0];
		String mValueStr = "AgeFile," + line[1];

		context.write(new Text(mKeyStr), new Text(mValueStr));
	}
}