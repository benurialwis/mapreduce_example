package mapreduceaverage;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author benuri
 */
public class MapReduceAverage {

    public static class Map extends Mapper<LongWritable, Text, Text, FloatWritable> {
        private Text subject = new Text();
        private final static FloatWritable score = new FloatWritable(1);

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer lines = new StringTokenizer(value.toString(), "\n");

            while (lines.hasMoreTokens()) {
                StringTokenizer scores = new StringTokenizer(lines.nextToken(), ",");
                while(scores.hasMoreTokens()) {
                    subject.set(scores.nextToken());
                    score.set(Float.parseFloat(scores.nextToken()));
                    context.write(subject, score);
                }
            }
        }
    }


    public static class Reduce extends Reducer<Text, FloatWritable, Text, FloatWritable> {

        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            float sum = 0;
            int count = 0;

            for (FloatWritable val : values) {
                sum += val.get();
                count++;
            }
            float average = sum/count;
            context.write(key, new FloatWritable(average));
        }
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "averagescore");

        job.setJarByClass(MapReduceAverage.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
    
}
