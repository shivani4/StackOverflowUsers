package project.org;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;

public class UserCountSortingReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        int outSize=conf.getInt("outputSize",50);
        int curSize=conf.getInt("curSize",0);

        System.out.println("outputSize-"+outSize+",curSize"+curSize);

        if(outSize>curSize){
                for(Text val : values) {
                        if(outSize>curSize){
                                curSize++;
                                context.write(key, val);
                        }
                }
                conf.setInt("curSize",curSize);
        }

    }
}

