import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Sport
{
	public static class Map extends Mapper<LongWritable, Text, Text, Text>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split(",");
			line[0] = line[0] + " " +line[1];
			context.write(new Text(line[0]), new Text(line[2]));
		}
	}
	public static class dpart extends Partitioner<Text, Text>
	{
		public int getPartition(Text key,Text value,int nr)
		{
			String s = value.toString();
			if(s.equalsIgnoreCase("Basketball"))
				return 0;
			else if(s.equalsIgnoreCase("Football"))
				return 1;
			else
				return 2;
		} 
	}
	public static class Reduce extends Reducer<Text, Text, Text, Text> 
	{
		int cnt = 0;
		String str = "total no of records";
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			for(Text val : values)
			{
				context.write(key,val);
				cnt++;
			}
		}
		public void cleanup(Context context) throws IOException, InterruptedException
		{
			context.write(new Text(str), new Text(" : " + cnt));
		}
	}
	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "sport");
		job.setJarByClass(Sport.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setPartitionerClass(dpart.class);
		job.setNumReduceTasks(3);
		job.setInputFormatClass(TextInputFormat.class); 
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
}

