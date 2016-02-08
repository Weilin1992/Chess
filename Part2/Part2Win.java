import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
public class Part2Win {

  public static class Part2WinMapper
       extends Mapper<Object, Text, Text, Text>{
    //two players
    Text player1 = new Text();
    Text player2 = new Text();

    private Text one = new Text("1");
    private Text zero = new Text("0"); 
    private Text half = new Text("0.5");

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
    	try{
    		String[] lines= value.toString().split("\n");
    		for(int i=0;i<lines.length;i++)
    		{
    			String line = lines[i];
    			if(line.contains("White "))
    			{
    				String name = line.trim().replace("White ", "");
    				name=name.replaceAll("\"", "");
    				name=name.replaceAll("\\[", "");
    				name=name.replaceAll("\\]", "");
    				player1.set(name+" white");
    			}
    			else if(line.contains("Black "))
    			{
    				String name = line.trim().replace("Black ", "");
    				name=name.replaceAll("\"", "");
    				name=name.replaceAll("\\[", "");
    				name=name.replaceAll("\\]", "");
    				player2.set(name+" Black");
    			}
        		if(line.contains("Result"))
        		{
        			if(line.contains("0-1"))
        			{
        				context.write(player1, zero);
        				context.write(player2, one);
        			}
        			else if(line.contains("1-0"))
        			{
        				context.write(player1, one);
        				context.write(player2, zero);
        			}
        			else if(line.contains("1/2-1/2"))
        			{
        				context.write(player1, half);
        				context.write(player2, half);
        			}
        		}
    		}
    		
    		
    	}
    	catch(Exception e)
    	{
    		
    	}
    	
    }
  }

  public static class Part2WinCombiner
  extends Reducer<Text,Text,Text,Text> {

	public void reduce(Text key, Iterable<Text> values,
	                  Context context
	                  ) throws IOException, InterruptedException {
		
		int win=0;
		int lost=0;
		int draw=0;
		
		for(Text d:values)
		{
			if(d.toString().equals("1"))
				win+=1;
			else if(d.toString().equals("0"))
				lost+=1;
			else if(d.toString().equals("0.5"))
				draw+=1;
		}
		String score = win+"\t"+lost+"\t"+draw;
		Text t = new Text(score);
		//System.out.println(key.toString()+"  "+t.toString());
		context.write(key,t);
	}
}

  public static class Part2WinReducer
       extends Reducer<Text,Text,Text,Text> {
    private DoubleWritable result = new DoubleWritable();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	
		double win=0;
		double lost=0;
		double draw=0;
		for(Text d:values)
		{
			String[] score = d.toString().split("\t");
			win = Double.parseDouble(score[0]);
			lost = Double.parseDouble(score[1]);
			draw = Double.parseDouble(score[2]);
		}

		win = win/(win+lost+draw);
		win = Math.round(win*100.0)/100.0;
		lost = lost/(win+lost+draw);
		lost = Math.round(lost*100.0)/100.0;
		draw = draw/(win+lost+draw);
		draw = Math.round(draw*100.0)/100.0;
		Text t = new Text(win+"\t"+lost+"\t"+draw);
		context.write(key,t);
    	
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    conf.set("textinputformat.record.delimiter","}");

    Job job = Job.getInstance(conf, "Part2Win");
    job.setJarByClass(Part2Win.class);
    job.setMapperClass(Part2WinMapper.class);
    job.setCombinerClass(Part2WinCombiner.class);
    job.setReducerClass(Part2WinReducer.class);
    job.setOutputKeyClass(Text.class);
	//job2.setPartitionerClass(TotalOrderPartitioner.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true);
  }
}
