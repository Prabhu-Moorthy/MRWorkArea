package invertedindex.distributedcache;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
	enum customCounters{
		POSITIVE
	}
	Set<String> stopWords = new HashSet<String>();
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		String stopline = "";
		BufferedReader fis;
		String records = "";
		
		Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		String chunk = null;
		if(cacheFiles[0].toString().contains("stopwords")){
			fis = new BufferedReader(new FileReader(cacheFiles[0].toString()));
			while((chunk = fis.readLine()) != null){
				stopWords.add(chunk);
			}
		}
	}

	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		FileSplit split = (FileSplit)context.getInputSplit();
		String fileName = split.getPath().getName();
		String[] line = value.toString().split("\\s");

		for(String val:line){
			if(!stopWords.contains(val)){
				context.write(new Text(val), new Text(fileName));
			}
		}
	}
}