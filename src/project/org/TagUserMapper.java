package project.org;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
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

public class TagUserMapper extends Mapper<LongWritable, Text, Text,Text >{

        private static final Log LOG = LogFactory.getLog(TagUserMapper.class);


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

                String valueString = value.toString();
                String [] values = valueString.split("<830750047>");
                context.write(new Text(values[0].trim()),new Text(values[1].trim()));

        }
}
