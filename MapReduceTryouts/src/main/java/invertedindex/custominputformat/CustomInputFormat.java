package invertedindex.custominputformat;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


public class CustomInputFormat extends TextInputFormat{
	
@Override
	public RecordReader<LongWritable, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) {
		// TODO Auto-generated method stub
		return super.createRecordReader(split, context);
	}
@Override
	public List<InputSplit> getSplits(JobContext arg0) throws IOException {
		// TODO Auto-generated method stub
		return super.getSplits(arg0);
	}
@Override
protected boolean isSplitable(JobContext context, Path file) {
	// TODO Auto-generated method stub
	return super.isSplitable(context, file);
}
}