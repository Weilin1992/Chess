
buketname:zhaoweilin


Part1:first get the number of the White Black and Draw,then throught reducer to get the percentage


Part2:first get the name of the player + white/black/draw and the number of winning games,then get the
number of total games,and at last calculate the percentage

Part3:I start two MR jobs, the first output the number of each PlayCount, the use 
context.getCounter("org.apache.hadoop.mapred.Task$Counter",  "MAP_OUTPUT_RECORDS").getValue()
to get the number of matches,and set it as a global variable in conf
the second job is to calculate the percentage of each PlayCount and sort it by percentage
through  public static class SortKeyComparator extends WritableComparator, overwrite the 

