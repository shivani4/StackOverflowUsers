package project.org;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.nio.charset.CharacterCodingException;
import java.util.HashSet;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.xml.sax.InputSource;
import org.w3c.dom.*;
import javax.xml.xpath.*;

import java.io.IOException;
import java.io.StringReader;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.conf.Configuration;

public class UserCountMapper extends Mapper<LongWritable, Text, Text,Text >{


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

                        Configuration conf = context.getConfiguration();
                        HashSet<String> tags = new HashSet<String>();
                        for(int i=1;i<6;i++){
                                String tag =conf.get("tag"+i);
                                if(tag!=null && !tag.isEmpty())
                                        tags.add(tag);
                        }

                        String val=value.toString();
                        String tag=val.split("<TAG_830750047>")[0].trim();
                        String userCount=val.split("<TAG_830750047>")[1].trim();
                        String [] users = userCount.split("<830750047>");
                        for(int i=0;i<users.length;i++){
                                if(users[i]!=null && !users[i].isEmpty()){
                                        String u = users[i].split("\\$\\$")[0];
                                        String c =  users[i].split("\\$\\$")[1];
                                        if(tags.contains(tag))
                                                context.write(new Text(u), new Text(c));
                                }
                        }

        }

}
