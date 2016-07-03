package project.org;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.nio.charset.CharacterCodingException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.xml.sax.InputSource;
import org.w3c.dom.*;
import javax.xml.xpath.*;
import java.io.StringReader;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class UserCountSortingMapper extends Mapper<LongWritable, Text, Text,Text >{
	File file;
	HashMap<String, String> map = new HashMap<String, String>();
	@Override
	protected void setup(
	        Mapper<LongWritable, Text, Text, Text>.Context context)
	        throws IOException, InterruptedException {
	    if (context.getCacheFiles() != null && context.getLocalCacheFiles().length>0) {
	    	Path[] paths = context.getLocalCacheFiles();
	    	file=new File("./User_Mapping");
	    }
	    super.setup(context);
	}
	
	private void mapping_user() throws FileNotFoundException{
		
		Scanner s=new Scanner(file);
		while ((s.hasNextLine())) 
		{
			String sCurrentline = s.nextLine();
			StringTokenizer s1=new StringTokenizer(sCurrentline);
			if(s1.hasMoreTokens())
			{
				String key1=s1.nextToken();
				String value1=s1.nextToken();
				map.put(key1, value1);
				
			}
		}
			
	}
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String user=value.toString().split("<830750047>")[0].trim();
                mapping_user();
                user=map.get(user);
                String count=value.toString().split("<830750047>")[1].trim();
                context.write(new Text(count),new Text(user));
        }
}
