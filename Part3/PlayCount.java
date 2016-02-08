import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
public class PlayCount {

  
  public static int total = 0;

  public static class PlayCountMapper1
       extends Mapper<Object, Text, Text, IntWritable>{
  private final static IntWritable one = new IntWritable(1);
  private final static IntWritable zero = new IntWritable(0);
  String gameTime = new String();
	private Text count = new Text();
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      /*StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);*/

        try{
        	String line = value.toString();
        	if(line.contains("PlyCount"))
        	{
            total++;
            String name = line.trim().replace("PlyCount ", "");
            name=name.replaceAll("\"", "");
            name=name.replaceAll("\\[", "");
            name=name.replaceAll("\\]", "");
            count.set(name);
            context.write(count,one);
            total++;
        	}
        }
        catch(Exception e)
        {
        	e.printStackTrace();
        }
      }
    }

    public static class PlayCountMapper2
       extends Mapper<Object, Text, IntWritable, Text>{

  private Text count = new Text();
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
        try{
          String line = value.toString();
          String [] splits = line.split("\t");
          int sum = Integer.parseInt(splits[1]);
          Configuration conf = context.getConfiguration();
          String _Total = conf.get("total");
          int total = Integer.parseInt(_Total);
          System.out.println(total);
          System.out.println(sum);
          double percentage = (double) sum/total*100;
          context.write(new IntWritable(Integer.parseInt(splits[1])), new Text(splits[0] + "\t" + percentage + "%"));
        }
        catch(Exception e)
        {
          e.printStackTrace();
        }
      }
    }
  


  public static class PlayCountReducer1
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
    
      int sum = 0;
      
      long total = context.getCounter("org.apache.hadoop.mapred.Task$Counter",  "MAP_OUTPUT_RECORDS").getValue();
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }



  public static class PlayCountReducer2
       extends Reducer<IntWritable,Text,Text,IntWritable> {
    
    public void reduce(IntWritable key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
  
      for (Text val : values) {
        context.write(val,null);
      }
    }
  }


    public static class SortKeyComparator extends WritableComparator {
    protected SortKeyComparator() {
        super(IntWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        IntWritable first = (IntWritable) a;
        IntWritable second = (IntWritable) b;
        if(first.get() < second.get()) {
            return 1;
        }else if(first.get() > second.get()) {
            return -1;
        }else {
            return 0;
        }
    }
}

  public static void main(String[] args) throws Exception {
    Configuration conf1 = new Configuration();
    Job job1 = Job.getInstance(conf1, "Chess win");
    job1.setJarByClass(PlayCount.class);
    job1.setMapperClass(PlayCountMapper1.class);
    job1.setReducerClass(PlayCountReducer1.class);
    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(IntWritable.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path(args[1]));
    job1.waitForCompletion(true) ;
    System.out.println(job1.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter",  "MAP_OUTPUT_RECORDS").getValue());
    long total = job1.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter",  "MAP_OUTPUT_RECORDS").getValue();
    Configuration conf2 = new Configuration();
    String s = String.valueOf(total);
    conf2.set("total",s);
    Job job2 = Job.getInstance(conf2, "Chess win");
    job2.setJarByClass(PlayCount.class);
    job2.setMapperClass(PlayCountMapper2.class);
    job2.setPartitionerClass(TotalOrderPartitioner.class);
    //job.setCombinerClass(PlayCountReducer1.class);
    job2.setReducerClass(PlayCountReducer2.class);
    job2.setMapOutputKeyClass(IntWritable.class);
    job2.setMapOutputValueClass(Text.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(IntWritable.class);
    job2.setSortComparatorClass(SortKeyComparator.class);
    FileInputFormat.addInputPath(job2, new Path(args[1]));
    FileOutputFormat.setOutputPath(job2, new Path(args[2]));
    //job2.setPartitionerClass(TotalOrderPartitioner.class);
    job2.waitForCompletion(true) ;
    


  }
}
