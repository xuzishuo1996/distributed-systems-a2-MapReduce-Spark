package JFiles;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Task4 {

    // add code here
    public static class MovieRatingsMapper
            extends Mapper<Object, Text, Text, ArrayPrimitiveWritable> {

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",", -1);
            int[] ratings = new int[tokens.length - 1];
            for (int i = 1; i < tokens.length; ++i) {
                int rating = 0;
                if (!tokens[i].equals("")) {
                    rating = Integer.parseInt(tokens[i]);
                }
                ratings[i - 1] = rating;
            }
            context.write(new Text(tokens[0]), new ArrayPrimitiveWritable(ratings));
        }
    }

    public static class SimilarityReducer
            extends Reducer<Text, ArrayPrimitiveWritable, Text, IntWritable> {
        // TODO: movie-ratings HashMap. need concurrent or not?
        public static final ConcurrentHashMap<String, int[]> map = new ConcurrentHashMap<>();

        @Override
        public void reduce(Text key, Iterable<ArrayPrimitiveWritable> values, Context context)
                throws IOException, InterruptedException {
            String movie = key.toString();
            int[] ratings = (int[]) values.iterator().next().get();

            for (Map.Entry<String, int[]> entry: map.entrySet()) {
                StringBuilder sb = new StringBuilder();

                String movie2 = entry.getKey();
                if (movie.compareTo(movie2) < 0) {
                    sb.append(movie).append(',').append(movie2);
                } else {
                    sb.append(movie2).append(',').append(movie);
                }
                
                int[] ratings2 = entry.getValue();
                int similarity = 0;
                for (int i = 0; i < ratings.length; ++i) {
                    if (ratings[i] != 0 && ratings[i] == ratings2[i]) {
                        ++similarity;
                    }
                }
                
                context.write(new Text(sb.toString()), new IntWritable(similarity));
            }

            map.put(movie, ratings);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");

        Job job = Job.getInstance(conf, "Task4");
        job.setJarByClass(Task4.class);

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // add code here
        job.setMapperClass(Task4.MovieRatingsMapper.class);
        job.setReducerClass(Task4.SimilarityReducer.class);
        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ArrayPrimitiveWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
        TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
//        // for local test only
//        TextInputFormat.addInputPath(job, new Path("sample_input/smalldata.txt"));
//        TextOutputFormat.setOutputPath(job, new Path("my_output/java4.out"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
