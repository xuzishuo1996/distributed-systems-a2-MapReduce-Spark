package JFiles;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class Task2 {

    // add code here
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static Text dummy = new Text("");
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            for (int i = 1; i < tokens.length; i++) {
                if (!tokens[i].equals("")) {
                    context.write(dummy, one);
                }
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, NullWritable, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(null, new IntWritable(sum));
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");

        Job job = Job.getInstance(conf, "Task2");
        job.setJarByClass(Task2.class);

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // add code here
        job.setMapperClass(Task2.TokenizerMapper.class);
        job.setCombinerClass(Task2.IntSumReducer.class);
        job.setReducerClass(Task2.IntSumReducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(IntWritable.class);

//        TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
//        TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        // for local test only
        TextInputFormat.addInputPath(job, new Path("sample_input/smalldata.txt"));
        TextOutputFormat.setOutputPath(job, new Path("my_output/java2.out"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
