package stackoverflow.stack;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer1 extends Reducer<Text, Text, Text, Text> {
	protected void reduce(Text key, Iterable<Text> value,Context context)
			throws IOException, InterruptedException {
		String age = "";
		String sourceFile = "";
		List<Text> newList = new ArrayList<Text>();
		for(Text rVal:value){
			newList.add(new Text(rVal));
			String[]  splitString = rVal.toString().split(",");
			sourceFile = splitString[0];
			if(sourceFile.equals("AgeFile")){
				age = splitString[1];
			}
		}
		for(Text rVal:newList){
			String[]  splitString = rVal.toString().split(",");
			sourceFile = splitString[0];
			if(sourceFile.equals("GameFile")){
				String optValue = splitString[1].concat(",").concat(splitString[2]).concat(",").concat(age);
				System.out.println(optValue);
				context.write(key, new Text(optValue));
			}
		}
		//super.reduce(key, value, context);
	}
}
