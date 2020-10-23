//package JFiles;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

public class Task5 {

    // add code here

    public static class MaxMapper extends
            Mapper<Object, Text, Text, Text> {

        private static final Log LOG = LogFactory.getLog(Task5.MaxMapper.class);

        private static List<String[]> ratingOfMovies = new ArrayList<>();
        private BufferedReader reader;

        @Override
        protected void setup(Context context) throws IOException {


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

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

//            // for test only
//            LOG.error("enter mapper: " + value.toString()); // no log output here

            for (String[] tokens : ratingOfMovies) {

                Text movie = new Text(tokens[0]);

                StringBuilder maxUsers = new StringBuilder();
                int max = 0;
                for (int i = 1; i < tokens.length; ++i) {
                    if (tokens[i].equals("")) {
                        continue;
                    }
                    int rating = Integer.parseInt(tokens[i]);
                    if (rating == max) {
                        maxUsers.append(',').append(i);
                    } else if (rating > max) {
                        max = rating;
                        maxUsers = new StringBuilder();
                        maxUsers.append(i);
                    }
                }
                // Assume that there is at least one non-blank rating for each movie.
                context.write(movie, new Text(maxUsers.toString()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");

        Job job = Job.getInstance(conf, "Task1");
        job.setJarByClass(Task5.class);

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // add code here
        job.setMapperClass(Task5.MaxMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
        TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        // for remote only
        job.addCacheFile(new URI(otherArgs[0]));
//        // for local test only
//        TextInputFormat.addInputPath(job, new Path("sample_input/smalldata.txt"));
//        TextOutputFormat.setOutputPath(job, new Path("my_output/java1.out"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
