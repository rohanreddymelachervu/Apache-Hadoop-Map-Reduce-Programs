package KeyMatchReverse;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KeyReverse {
	// Mapper class and method
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text>{
	    private Text word = new Text();
	    private Text word2 = new Text();
	    public void map(LongWritable key, Text value, Context context) 
	           throws IOException, InterruptedException {
	      // Splitting the line on spaces
	      String[] stringArr = value.toString().split("\\s+");
	      for (String str : stringArr) {
	        word.set(str);
	        StringBuilder newstr  =new StringBuilder();
	        word2.set(newstr.append(str).reverse().toString());
	        context.write(word,word2);
	      }           
	    }
	  }
	    
	  // Reducer class and method
	  public static class MyReducer extends Reducer<Text, Text, Text, Text>{        
	    public void reduce(Text key, Iterable<Text> values, Context context) 
	            throws IOException, InterruptedException {
	      
	      for (Text val : values) {
	    	  context.write(key, val);
	    	  break;
	      }
	      
	      
	    }
	  }
	  public static void main(String[] args)  throws Exception{
	    Configuration conf = new Configuration();

	    Job job = Job.getInstance(conf, "WC");
	    job.setJarByClass(KeyReverse.class);
	    job.setMapperClass(MyMapper.class);    
	    job.setReducerClass(MyReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}

