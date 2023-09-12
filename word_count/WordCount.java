// Java imports
import java.io.IOException;
import java.util.StringTokenizer;

// Hadoop imports
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// WordCount class
public class WordCount {

  // This is the mapper class
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1); // To count the number of words
    private Text word = new Text(); // To store the word

    // This is the map function
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString()); // Tokenize the input
      // For each word, emit the word and 1
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }

  // This is the reducer class
  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable(); // To store the sum of the words

    // This is the reduce function
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0; // To store the sum of the words
      // For each word, add the count
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum); // Set the result
      context.write(key, result); // Emit the word and the sum
    }
  }

  // This is the main function
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration(); // Create a new configuration
    Job job = Job.getInstance(conf, "word count"); // Create a new job
    job.setJarByClass(WordCount.class); // Set the jar by class
    job.setMapperClass(TokenizerMapper.class); // Set the mapper class
    job.setCombinerClass(IntSumReducer.class); // Set the combiner class
    job.setReducerClass(IntSumReducer.class); // Set the reducer class
    job.setOutputKeyClass(Text.class); // Set the output key class
    job.setOutputValueClass(IntWritable.class); // Set the output value class
    FileInputFormat.addInputPath(job, new Path(args[0])); // Set the input path
    FileOutputFormat.setOutputPath(job, new Path(args[1])); // Set the output path
    System.exit(job.waitForCompletion(true) ? 0 : 1); // Wait for the job to complete
  }
}