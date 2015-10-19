package org.ncdc.MaxTemp;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

public class MapperTest{
	@Test
	public void checkTemperatureFieldPos() throws IOException{
		new MapDriver<LongWritable, Text, Text, IntWritable>()
		.withMapper(new ClimateMapper())
		.withInput(new LongWritable(0),new Text("0029029070999991901010106004+64333+023450FM-12+000599999V0202701N015919999999N0000001N9-00781+99999102001ADDGF108991999999999999999999"))
		.withOutput(new Text("1901"), new IntWritable(369))
		.runTest();
	}

}
