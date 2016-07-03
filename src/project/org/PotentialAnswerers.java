package project.org;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.DoubleWritable;
import java.io.IOException;
public class PotentialAnswerers extends Configured implements Tool {


            public static void main(String[] args) throws Exception {
                        System.exit(ToolRunner.run(new Configuration(), new PotentialAnswerers(), args));

                }

                @Override
                public int run(String[] args) throws Exception {

                                boolean isCompleted = parseQuesAns(args[0], args[1]);
                                if (!isCompleted) return 1;

                                isCompleted = parseUsers(args[0], args[1]);
                                if (!isCompleted) return 1;

                                return 0;
                }

                boolean parseQuesAns(String inputpath ,String outputPath) throws IOException, ClassNotFoundException, InterruptedException {

                                boolean isCompleted = parseXML(inputpath+"/posts/",outputPath+"/parsing");
                                if (!isCompleted) return false;
                                isCompleted = computeTagsUserMapping(outputPath+"/parsing/part-r-00000",outputPath+"/computation");
                                if (!isCompleted) return false;
                                        return true;
                }

                boolean parseUsers(String inputpath ,String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
                                 boolean isCompleted = parseUsersXML(inputpath+"/users/",outputPath+"/users");
                                 if (!isCompleted) return false;
                                        return true;
                }

         private boolean parseUsersXML(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
                 Configuration conf = new Configuration();
                 conf.set(XmlInputFormat.START_TAG_KEY, "<row");
                 conf.set(XmlInputFormat.END_TAG_KEY, " />");
                 Job job = Job.getInstance(conf, "users parsing");
                 job.setJarByClass(PotentialAnswerers.class);
                 FileSystem fs = FileSystem.get(conf);
                 fs.delete(new Path(outputPath), true);
                 FileInputFormat.addInputPath(job, new Path(inputPath));
                 job.setInputFormatClass(XmlInputFormat.class);
                 job.setMapperClass(UserParserMapper.class);

                 FileOutputFormat.setOutputPath(job, new Path(outputPath));

                 job.setOutputFormatClass(TextOutputFormat.class);
                 job.setOutputKeyClass(Text.class);
                 job.setOutputValueClass(Text.class);
                //job.setReducerClass(UsersParserReducer.class);
                 return job.waitForCompletion(true);
    }



        private boolean parseXML(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
                Configuration conf = new Configuration();
                conf.set(XmlInputFormat.START_TAG_KEY, "<row");
                conf.set(XmlInputFormat.END_TAG_KEY, " />");
                Job job = Job.getInstance(conf, "posts parsing");
                job.setJarByClass(PotentialAnswerers.class);
                FileSystem fs = FileSystem.get(conf);
                fs.delete(new Path(outputPath), true);
                FileInputFormat.addInputPath(job, new Path(inputPath));
                job.setInputFormatClass(XmlInputFormat.class);
                job.setMapperClass(PostsParserMapper.class);

                FileOutputFormat.setOutputPath(job, new Path(outputPath));
                                                                                                                                                      
                FileOutputFormat.setOutputPath(job, new Path(outputPath));

                job.setOutputFormatClass(TextOutputFormat.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                job.setReducerClass(PostsParserReducer.class);
                return job.waitForCompletion(true);
    }

         private boolean computeTagsUserMapping (String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
                Configuration conf = new Configuration();
                Job job = Job.getInstance(conf, "computation");
                job.setJarByClass(PotentialAnswerers.class);
                FileSystem fs = FileSystem.get(conf);
                fs.delete(new Path(outputPath), true);
                FileInputFormat.addInputPath(job, new Path(inputPath));
                job.setInputFormatClass(TextInputFormat.class);
                job.setMapperClass(TagUserMapper.class);

                FileOutputFormat.setOutputPath(job, new Path(outputPath));

                job.setOutputFormatClass(TextOutputFormat.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                job.setReducerClass(TagUserReducer.class);
                return job.waitForCompletion(true);


                }

}
                                              