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
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.DoubleWritable;

import java.io.IOException;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.io.*;


public class PredictAnswerers extends Configured implements Tool {

	    public static void main(String[] args) throws Exception {
			Scanner s = new Scanner(System.in);
			System.out.println("Please Enter your question:-");
			String question= s.nextLine();
			System.out.println("Enter Tags:-");
			String [] tags = new String [5];
			int nonEmpty=0;
			for(int i=0;i<5;i++){
				System.out.println("Tag"+(i+1)+":-");
				tags[i]=s.nextLine();
				if(tags[i]!=null && !tags[i].isEmpty())
					nonEmpty++;
			}	
			
			String [] totalArgs= new String[6];
			totalArgs[0]=args[0];
			
			for(int i=0;i<5;i++){
				totalArgs[i+1]=tags[i];	
			}
		
			System.exit(ToolRunner.run(new Configuration(), new PredictAnswerers(), totalArgs));
   	
		}
		
		@Override
		public int run(String[] args) throws Exception {
					
				boolean isCompleted = false;
				try {
					isCompleted = predictAnswerers(args);
				} catch (Throwable e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				if (!isCompleted) return 1;				
				return 0;
		}


		boolean predictAnswerers(String [] args ) throws Throwable {
				 boolean isCompleted = parseTagsUsers(args[0]+"/computation/part-r-00000",args[0]+"/prediction",args);
	                         if (!isCompleted) return false;
				 
				 isCompleted = sortUsers(args[0]+"/prediction/part-r-00000",args[0]+"/finalOutput",args);
	                         if (!isCompleted) return false;
					return true;	
				 
		}
	

	 private boolean parseTagsUsers(String inputPath, String outputPath,String [] args ) throws IOException, ClassNotFoundException, InterruptedException {
                Configuration conf = new Configuration();
		for(int i=1;i<args.length;i++){
			conf.set("tag"+i, args[i]);											
		}
			
                Job job = Job.getInstance(conf, "prediction");
                job.setJarByClass(PredictAnswerers.class);
                FileSystem fs = FileSystem.get(conf);
                fs.delete(new Path(outputPath), true);
                FileInputFormat.addInputPath(job, new Path(inputPath));
                job.setInputFormatClass(TextInputFormat.class);
                job.setMapperClass(UserCountMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
                FileOutputFormat.setOutputPath(job, new Path(outputPath));
                //job.setOutputFormatClass(IntOutputFormat.class);
		//job.setSortComparatorClass(DescendingIntComparator.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                job.setReducerClass(UserCountReducer.class);
                return job.waitForCompletion(true);                                  
                
        }
	 private boolean sortUsers(String inputPath, String outputPath,String [] args ) throws IOException, ClassNotFoundException, InterruptedException, Throwable {
                Configuration conf = new Configuration();
                conf.setInt("outputSize",200);
                conf.setInt("curSize",0);		
                Job job = Job.getInstance(conf, "sorting");
                job.setJarByClass(PredictAnswerers.class);
                FileSystem fs = FileSystem.get(conf);
                fs.delete(new Path(outputPath), true);
                FileInputFormat.addInputPath(job, new Path(inputPath));
                job.setInputFormatClass(TextInputFormat.class);
                job.setMapperClass(UserCountSortingMapper.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);
                FileOutputFormat.setOutputPath(job, new Path(outputPath));
                //job.setOutputFormatClass(IntOutputFormat.class);
                job.setSortComparatorClass(DescendingIntComparator.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                job.setReducerClass(UserCountSortingReducer.class);
                job.addCacheFile(new URI("/project/output/users/part-r-00000" + "#User_Mapping"));
                return job.waitForCompletion(true);
        }

}
