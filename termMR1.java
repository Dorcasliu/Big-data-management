package comp9313.proj1;
  
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
   
           
public class termMR1 
{
    public static class Map extends Mapper<LongWritable, Text, StringPair, IntWritable>
    {
    private final static IntWritable one = new IntWritable(1);
       
    private StringPair wordPair = new StringPair();
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
    {

    	String doc = value.toString();//each line/file
        //StringTokenizer doc = new StringTokenizer(value.toString(), " *$&#/\t\n\f\"'\\,.:;?![](){}<>~-_"); //each line/file
        String docPart[] = doc.split(" "); //split input string to get individual words
        String docID = docPart[0]; 
        String tempStr=""; //to construct the key part
        HashSet<String> hs = new HashSet<String>();
        
        //loop to collect all the words
        for(int i=1;i<docPart.length;i++)
        {
        	tempStr = docPart[i].toLowerCase();
        	wordPair.set(tempStr, docID);
        	context.write(wordPair, one);
        	
        	//emit one more special key for counting how many document contains that term.
            if (!hs.contains(tempStr)){
           		hs.add(tempStr);
             	wordPair.set(tempStr, "*");
            	context.write(wordPair, one);
            }
        }
    }
}
   
    public static class Combiner extends Reducer<StringPair, IntWritable, StringPair, IntWritable> {
           
        private IntWritable result = new IntWritable();
            
        public void reduce(StringPair key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
           }
            result.set(sum);
            context.write(key, result);
        }       
    }
        
    public static class Partitioners extends Partitioner<StringPair, IntWritable>{
            
        public int getPartition(StringPair key, IntWritable value, int numPartitions) {
            return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }     
    }    
       
    
    public static class Reduce extends Reducer<StringPair, IntWritable, StringPair, DoubleWritable> {
    	
	    private DoubleWritable result = new DoubleWritable();
	    private double ni = 0;
	    private StringPair termPair = new StringPair();
	    
	    public void reduce(StringPair key, Iterable<IntWritable> values, Context context
	    		) throws IOException, InterruptedException {
        int sum = 0;//ni
        for (IntWritable val : values) 
        {
            sum += val.get();
        }
        
        Configuration conf = context.getConfiguration();
        long N =0;
        N =conf.getLong("FileRecorder",0);
         
        String second = key.getSecond();  

        if(!second.equals("*")){
        	double idf = Math.log10(N/ni);
        	String key_1 = key.getFirst()+'\t';//change the format as the output example.
        	termPair.set(key_1, second);
            double TFIDF = sum*idf;
            result.set(TFIDF);
            context.write(termPair, result);
        }else{
            //when it's a special key, record its value, which is exactly DF.
        		 ni = sum;//ni
        }
        }                  
        }
}