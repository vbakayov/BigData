
import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount  extends Configured implements Tool{
	public static void main(String[] args){
		String filename = "my.jar";
		String workingDirectory = System.getProperty("user.dir");
		String absoluteFilePath = "";	
		absoluteFilePath = workingDirectory + File.separator + filename;

		Configuration conf = new Configuration();
		
		conf.addResource(new Path(".."+File.separator+"conf"+"core-site.xml"));
		conf.set("mapred.jar", absoluteFilePath);
		
		
		try {
			ToolRunner.run(conf, new WordCount(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job  = new Job(getConf());
		job.setJobName("WordCount-v1");
		job.setJarByClass(WordCount.class);
		
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.submit();
		
		return (job.waitForCompletion(true) ? 0 : 1);
		
	}
	
	public static class Map extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{ 
			String line = value.toString();
			StringTokenizer  tokenizer = new StringTokenizer(line);
			while(tokenizer.hasMoreTokens()){
				word.set(tokenizer.nextToken());
				context.write(word,one);
				//this is a test
				}
			}
			
		 
		}  
		
	public static class Reduce extends org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, IntWritable>{
		public void reduce(Text key, Iterable<IntWritable>values, Context context) throws IOException,InterruptedException{
			int sum = 0; 
			for (IntWritable value: values)  
				sum += value.get(); 
			context.write(key, new IntWritable(sum)); 
		}
	}
	
	}