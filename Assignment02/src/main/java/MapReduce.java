import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class MapReduce {
    public static class CharMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text name = new Text("Characters");

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            int length = line.length();
            context.write(name, new IntWritable(length));
        }
    }

    public static class WordMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text name = new Text("Words");

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                itr.nextToken();
                context.write(name, one);
            }
        }
    }

    public static class LineMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text name = new Text("Lines");

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            context.write(name, one);
        }
    }

    public static class NameMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private final Text romeo = new Text("Romeo");
        private final Text juliet = new Text("Juliet");

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()) {
                String name = itr.nextToken().replaceAll("\\W", "");
                if (name.equalsIgnoreCase("romeo")) {
                    context.write(romeo, one);
                } else if (name.equalsIgnoreCase("juliet")) {
                    context.write(juliet, one);
                }
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Count");
        job.setJarByClass(MapReduce.class);

        // job.setMapperClass(CharMapper.class);
        // job.setMapperClass(WordMapper.class);
        // job.setMapperClass(LineMapper.class);
        job.setMapperClass(NameMapper.class);

        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}