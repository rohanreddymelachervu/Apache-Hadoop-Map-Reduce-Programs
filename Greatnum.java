import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class Greatnum {
	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	    private Text word = new Text();
	    private Text word2 = new Text();
	    public void map(LongWritable key, Text value, Context context) 
	           throws IOException, InterruptedException {
	      String[] stringArr = value.toString().split("\\s+");
	      	int greatest=0;
		word.set("greatest");
		for (String str : stringArr) 
		{
	        int n=Integer.parseInt(str);
	        if(n>greatest)
		{
	           greatest=n;
		}
		}     
		context.write(word,new IntWritable(greatest));      
		
	    }
	  }
	  public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>{        
	    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
	            throws IOException, InterruptedException {
	      int greatest=0;
	      for (IntWritable val : values) {
	    	  if(val.get()>greatest)
			  {
                    greatest=val.get();
	          }
	      }
	      context.write(key,new IntWritable(greatest));
	      
	    }
	  }
	  public static void main(String[] args)  throws Exception{
	    Configuration conf = new Configuration();

	    Job job = Job.getInstance(conf, "WC");
	    job.setJarByClass(Greatnum.class);
	    job.setMapperClass(MyMapper.class);    
	    job.setReducerClass(MyReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}

