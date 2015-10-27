package gamefile.distributedcache;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class GameMapper extends Mapper<LongWritable, Text, Text, Text> {
	private static Map<String,String> ageTable = new HashMap<String,String>();
	private static Map<String,String> gameTable = new HashMap<String,String>();
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		//Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		URI[] uriList = DistributedCache.getCacheFiles(context.getConfiguration());
		BufferedReader br = new BufferedReader(new FileReader(uriList[0].getPath()));
		String strReadLine = "";
		/*for(Path eachPath:cacheFilesLocal){
			if(eachPath.getName().toString().trim().equals("Age.csv")){
				BufferedReader br = new BufferedReader(new FileReader(eachPath.toString()));
				while ((strReadLine = br.readLine()) != null) {
					String[] ageFile = strReadLine.split(",");
					ageTable.put(ageFile[0].trim(), ageFile[1].trim());
				}
			}
		}*/
	}

	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		String[] line = value.toString().split(",");
		String mKeyStr = line[0];
		String mValueStr = line[1] + "," + line[2];
		String outputValue = new String();
		
		if(ageTable.get(mKeyStr) != null){
			String age = ageTable.get(mKeyStr);
			outputValue = mValueStr + "," + age;
			context.write(new Text(mKeyStr), new Text(outputValue));
		}
	}
}