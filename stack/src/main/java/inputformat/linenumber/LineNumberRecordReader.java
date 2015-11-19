package inputformat.linenumber;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class LineNumberRecordReader extends LineRecordReader {
	private LongWritable key;
	private Text value;
	private static Long lineNumber = new Long(1);
	
	@Override
	public boolean nextKeyValue() throws IOException {
		super.nextKeyValue();
		key = new LongWritable(lineNumber);
		value = super.getCurrentValue();
		lineNumber++;
		return true;
	}
	
	@Override
	public LongWritable getCurrentKey() {
		// TODO Auto-generated method stub
		return key;
	}
}
