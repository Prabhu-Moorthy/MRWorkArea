package regex.sample;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class VogellaTryouts {

	public static void main(String[] args) {
		String line = "Prabhu Cloudera Certified";
		String pattern = "\\w*\\s\\w*\\s\\w*";
		
		Pattern p = Pattern.compile(pattern);
		Matcher m = p.matcher(line);
		
		if(m.matches()){
			System.out.println("Certified");
		}

	}

}
