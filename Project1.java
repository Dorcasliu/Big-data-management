package comp9313.proj1;
  
import java.io.IOException;

/**This project is for computing the term weights by TF-IDF.
 * which is achieved by using two times MAPPER and REDUCER. 
 * The first one is for counting the number of documents, which is achieved in MAPPER by setting an enum FileRecorder.
 * In this circle the output file contained nothing.  
 * The second time is for counting TF,which is the number of times the term appeared in this document, 
 * this is achieved by setting the key and value, the key contains the term and it's document ID.
 * Also it did the job of counting DF, which is the number of documents contains this term,
 * this is achieved by setting an special key, the key is the term and value is *.
 * @author Yujie.Liu
 * @studentID z5124388 
 */
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
 
import comp9313.proj1.termMR1.Combiner;
import comp9313.proj1.termMR1.Map;
import comp9313.proj1.termMR1.Partitioners;
import comp9313.proj1.termMR1.Reduce;
  
  
public class Project1 {
    //do the job of counting N, which is the number of documents.  
    public static class Map1 extends Mapper<LongWritable, Text, StringPair, IntWritable>
    {           
        public static enum FileRecorder{
            TotalRecoder
        }   
  
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
            {
            //String doc = value.toString();//each line/file
            context.getCounter("Recorder", "N").increment(1);
            }
    }
          
    public static void main(String[] args) throws Exception {
	    Configuration conf1 = new Configuration();
	    Job job1 = Job.getInstance(conf1, "Project1");
	    job1.setJarByClass(Project1.class);
	    job1.setMapperClass(Map1.class);
	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(IntWritable.class);
	    //job1.setNumReduceTasks(1);
	    
	     
	    FileInputFormat.addInputPath(job1, new Path(args[0]));
	    
	    //this output is of the first mapreduce,just for counting # of documents, thus the output file is empty.  
	    FileOutputFormat.setOutputPath(job1, new Path(args[0]+"output_nothing_but_counting_N"));//N
	    job1.waitForCompletion(true);
	    
	    int number_reducer=Integer.parseInt(args[2]);//set the number of reducers as the user's input 
	        
	    Configuration conf = new Configuration();
	    conf.set("mapred.textoutputformat.separator", ","); 
	    conf.setLong("FileRecorder",job1.getCounters().findCounter("Recorder","N").getValue());
	    Job job = Job.getInstance(conf, "termMR1");
	    
	 
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    job.setJarByClass(termMR1.class);
	    
	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reduce.class);
	 
	    job.setCombinerClass(Combiner.class);
	    job.setPartitionerClass(Partitioners.class);
	    job.setMapOutputKeyClass(StringPair.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    
	    job.setOutputKeyClass(StringPair.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    job.setNumReduceTasks(number_reducer);         
	 
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	        
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	 
	    System.exit(job.waitForCompletion(true) ? 0 : 1);         
 }
}
