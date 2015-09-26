package stackoverflow.stack;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

public class ReducerTest {

	@Test
	public void processValidRecords() throws IOException{
		Reducer1 r = new Reducer1();
		Text iKey = new Text("11");
		Text val1 = new Text("GameFile,Mario,100");
		Text val2 = new Text("GameFile,Contra,97");
		Text val3 = new Text("GameFile,Tank,86");
		Text val4 = new Text("AgeFile,18");
		List<Text> iValues = Arrays.asList(val1,val2,val3,val4);
		
		Text rOpt = new Text("Mario,100,18");
		Text rOpt1 = new Text("Contra,97,18");
		Text rOpt2 = new Text("Tank,86,18");
		new ReduceDriver<Text, Text, Text, Text>()
		.withReducer(r)
		.withInput(iKey,iValues)
		.withOutput(iKey, rOpt)
		.withOutput(iKey, rOpt1)
		.withOutput(iKey, rOpt2)
		.runTest();
		
	}
	
	@Test
	public void testGameREducer2() throws IOException{
		Text mKey = new Text("Tank");
		IntWritable val1 = new IntWritable(18);
		IntWritable val2 = new IntWritable(26);
		
		List<IntWritable> mVal = Arrays.asList(val1,val2);
		
		
		
		new ReduceDriver<Text, IntWritable, Text, IntWritable>()
		.withReducer(new Reducer2())
		.withInput(mKey,mVal)
		.withOutput(mKey,new IntWritable(22))
		.runTest();
	}
}
