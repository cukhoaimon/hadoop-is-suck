import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TFIDF {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] parts = value.toString().split("\\s+");
            String docId = parts[0];
            String freq = parts[1];

            context.write(
                    new Text(docId),
                    new Text(key.toString() + " " + freq)
            );
        }
    }

    public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
        static int totalDoc = 0;
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // total terms in a doc
            int count = 0;
            Map<String, Integer> aggKeys = new HashMap<>();
            for (Text value : values) {
                count++;
                String[] parts = value.toString().split("\\s+");
                //              termId              docId
                String aggKey = parts[0] + " " + key.toString();
                aggKeys.put(aggKey, Integer.parseInt(parts[1]));
            }

            int finalCount = count;
            for (Map.Entry<String, Integer> entry : aggKeys.entrySet()) {
                float tf = (float) entry.getValue() / finalCount;
                context.write(
                        new Text(entry.getKey()),
                        new Text(Float.toString(tf))
                );
            }

            totalDoc++;
        }

        @Override
        protected void cleanup(Context context) {
            Configuration config = context.getConfiguration();
            config.set("totalDoc", Integer.toString(totalDoc));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "tf");
        job.setJarByClass(TFIDF.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1] + ".tmp"));

        job.waitForCompletion(true);

        // ------------------------
        Job job2 = Job.getInstance(conf, "tdf");
        job2.setJarByClass(TFIDF.class);

        job2.setMapperClass(TokenizerMapper.class);
        job2.setReducerClass(IntSumReducer.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setInputFormatClass(KeyValueTextInputFormat.class);

        FileInputFormat.addInputPath(job2, new Path(args[1] + ".tmp"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}