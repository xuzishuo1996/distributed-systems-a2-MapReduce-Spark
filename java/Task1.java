package java;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Task1 {

  // add code here
  public static class MaxMapper extends Mapper<Object, Text, Text, Text> {

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
      String[] tokens = value.toString().split(",");
      Text movie = new Text(tokens[0]);
      List<Integer> maxUserList = new ArrayList<>();
      int max = 0;
      for (int i = 0; i < tokens.length; i++) {
        int rating = Integer.parseInt(tokens[i]);
        if (rating == max) {
          maxUserList.add(i);
        } else if (rating > max) {
          max = rating;
          maxUserList = new ArrayList<>(Collections.singletonList(i));
        }
      }
      // Assume that there is at least one non-blank rating for each movie.
      context.write(movie, (Text) maxUserList);
    }

    public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();
      conf.set("mapreduce.output.textoutputformat.separator", ",");

      Job job = Job.getInstance(conf, "Task1");
      job.setJarByClass(Task1.class);

      String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

      // add code here
      job.setMapperClass(Task1.MaxMapper.class);

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

//      TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
//      TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
      // for local test only
      TextInputFormat.addInputPath(job, new Path("../sample_input/smalldata.txt"));
      TextOutputFormat.setOutputPath(job, new Path("../my_output/java1.out"));

      System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
  }
}
