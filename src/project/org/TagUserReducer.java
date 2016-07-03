package project.org;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.*;
public class TagUserReducer extends Reducer<Text, Text, Text, Text> {

        @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String,Integer> userCountMap = new HashMap<String,Integer>();
        for(Text val : values) {
                String user=val.toString();
                if(userCountMap.containsKey(user))
                        userCountMap.put(user,userCountMap.get(user)+1);
                else
                        userCountMap.put(user,1);
        }

        StringBuilder userCountString = new StringBuilder()     ;
        for(String user :userCountMap.keySet()){
                int count = userCountMap.get(user);
                userCountString.append(user+"$$"+count+"<830750047>");
        }
        context.write(key, new Text("<TAG_830750047>"+userCountString.toString()));
    }
}
