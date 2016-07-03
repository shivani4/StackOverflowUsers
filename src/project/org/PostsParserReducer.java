package project.org;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PostsParserReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                String str_send = "";
                String userid = "";
                String del = "";
                //String check = "";
                List<String> users = new ArrayList<String>();
                List<String> tags = new ArrayList<String>();
                for (Text value : values) {
                        String str=value.toString();
                       if(str.split("<830750047>")[1].equals("<NULL_830750047>"))
                       {
                    	   users.add(str.split("<830750047>")[0]);
                       }                       
                       else{
                    	   String tagsv=str.split("<830750047>")[1];
                    	   StringTokenizer str1 = new StringTokenizer(tagsv, "><");
                           if (str1.hasMoreTokens())
                                   del = str1.nextToken();
                           if(str1.hasMoreTokens())
                           {                 
                                                         while (str1.hasMoreTokens()) {
                                                           del=str1.nextToken();
                                                           tags.add(del);
                                                           }
                             }
                       }
                }
                  for(String tag:tags){
                	  for(String user:users){
                          context.write(new Text(tag), new Text("<830750047>"+user));
                  }
                     
                       
                }
        }
}
