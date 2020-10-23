//package JFiles;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class Task4 {
//    private static String inputPath;

    public static class SimilarityMapper extends
            Mapper<Object, Text, Text, IntWritable> {

        private static final Log LOG = LogFactory.getLog(SimilarityMapper.class);

        private static final List<String[]> ratingOfMovies = new ArrayList<>();
        private BufferedReader reader;

        @Override
        public void setup(Context context) throws IOException {

            URI[] localURIs = context.getCacheFiles();

            for (URI uri : localURIs) {
//                if (uri.toString().trim().equals(inputPath)) {
//                }
                LOG.error("cache file name: " + uri.toString());
                loadMovieRatings(new Path(uri));
            }
        }

        public void loadMovieRatings(Path filePath) throws IOException {
            String line;
            try {
                reader = new BufferedReader(new FileReader(filePath.toString()));

                while ((line = reader.readLine()) != null) {
                    String[] movieRatings = line.split(",", -1);
                    ratingOfMovies.add(movieRatings);
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (reader != null) {
                    reader.close();
                }
            }
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            LOG.error("enter mapper: " + value.toString());

            String[] ratings1 = value.toString().split(",", -1);
            for (String[] ratings2: ratingOfMovies) {
                int similarity = 0;
                if (ratings1[0].compareTo(ratings2[0]) < 0) {
                    for (int i = 1; i < ratings1.length; ++i) {
                        if (!ratings1[i].equals("") && ! ratings2[i].equals("")) {
                            int rating1 = Integer.parseInt(ratings1[i]);
                            int rating2 = Integer.parseInt(ratings2[i]);
                            if (rating1 == rating2) {
                                ++similarity;
                            }
                        }
                    }
                    String outputKey = ratings1[0] + "," + ratings2[0];
                    context.write(new Text(outputKey),new IntWritable(similarity));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Log LOG = LogFactory.getLog(Task4.class);
        LOG.error("START IN MAIN!!!");

        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");

        Job job = Job.getInstance(conf, "Task4");
        job.setJarByClass(Task4.class);

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//        inputPath = otherArgs[0];
        // for remote only
        job.addCacheFile(new URI(otherArgs[0]));

        // add code here
        job.setMapperClass(SimilarityMapper.class);
        job.setNumReduceTasks(0);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
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
