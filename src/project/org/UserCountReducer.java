package project.org;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.*;
import java.util.*;

public class UserCountReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int sum=0;
        for(Text val : values) {
                sum+=Integer.parseInt(val.toString());
        }

        context.write(key, new Text("<830750047>"+sum));
    }
}
