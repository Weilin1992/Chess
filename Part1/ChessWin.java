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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ChessWin {

  public static class ChessWinMapper
       extends Mapper<Object, Text, Text, IntWritable>{

  private final static IntWritable one = new IntWritable(1);
  private final static IntWritable zero = new IntWritable(0);
  private Text Draw = new Text("Draw");
	private Text White = new Text("White");
	private Text Black = new Text("Black");
	private Text Total = new Text("Total");
	
	int test_game = 1;
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      /*StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);*/
        try{
        	String line = value.toString();
        	if(line.contains("Result"))
        	{
        		test_game++;
        		if(line.contains("0-1"))
        		{
        			context.write(Black,one);
              context.write(White,zero);
              context.write(Draw,zero);
        		}
        		if(line.contains("1-0"))
        		{
        			context.write(Black,zero);
              context.write(White,one);
              context.write(Draw,zero);
        		}
        		if(line.contains("1/2-1/2"))
        		{
        			context.write(Black,zero);
              context.write(White,zero);
              context.write(Draw,one);
        		}
        	}
        }
        catch(Exception e)
        {
        	e.printStackTrace();
        }
      }
    }
  

  public static class ChessWinReducer1
       extends Reducer<Text,IntWritable,Text,Text> {
    
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      double sum = 0;
      double total = 0;
      double percentage = 0;
      for (IntWritable val : values) {
        sum += val.get();
        total++;
      }
      percentage = sum/total;
      percentage = Math.round(percentage*100.0)/100.0;
      //String res = String.valueOf(sum) + "\t" + String.valueOf(percentage);
      String res = "" + sum + "\t" + percentage;
      Text t = new Text(res);
      context.write(key, t);
    }
  }

  public static class ChessWinReducer2
       extends Reducer<Text,Text,Text,Text> {
    
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      String res = new String();
      for(Text d:values)
      {
        res = d.toString();
      } 
      Text t = new Text(res);
      context.write(key, t);
    }
  }


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Chess win");
    job.setJarByClass(ChessWin.class);
    job.setMapperClass(ChessWinMapper.class);
    //job.setCombinerClass(ChessWinReducer1.class);
    job.setReducerClass(ChessWinReducer1.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
  
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
