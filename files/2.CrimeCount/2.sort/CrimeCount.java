import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CrimeCount {

    public static class Mapper1
            extends Mapper<Object, Text, Text, LongWritable> {

        private final static LongWritable one = new LongWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            int arrSize = fields.length;

            if (arrSize == 20) {
                if (fields[12].matches("-?\\d+(\\.\\d+)?"))
                    word.set(fields[12]);

            } else if (arrSize == 21) {
                if (fields[13].matches("-?\\d+(\\.\\d+)?"))
                    word.set(fields[13]);
   } else if (arrSize == 22) {
                if (fields[14].matches("-?\\d+(\\.\\d+)?"))
                    word.set(fields[14]);

            } else if (arrSize == 23) {
                if (fields[12].matches("-?\\d+(\\.\\d+)?"))
                    word.set(fields[12]); // should be removed

            } else if (arrSize == 24) {
                if (fields[12].matches("-?\\d+(\\.\\d+)?"))
                    word.set(fields[12]);

            } else if (arrSize == 25) {
                if (fields[13].matches("-?\\d+(\\.\\d+)?"))
                    word.set(fields[13]);

            } else if (arrSize == 26) {
                if (fields[14].matches("-?\\d+(\\.\\d+)?"))
                    word.set(fields[14]);

            } else if (arrSize == 28) {
                if (fields[16].matches("-?\\d+(\\.\\d+)?"))
                    word.set(fields[16]);

            }

            context.write(word, one);
        }
    }

    public static class Reducer1
            extends Reducer<Text, LongWritable, Text, LongWritable> {
        private LongWritable result = new LongWritable();

        public void reduce(Text key, Iterable<LongWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class Mapper2
            extends Mapper<Object, Text, LongWritable, Text> {

        long max_val = 1000 * 1000;
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
   String [] splits = value.toString().split("\\s+");
   if (splits.length == 2) {
            String real_key = splits[0];
            String real_value = splits[1];
   long numeric_value = Integer.valueOf(real_value);
            long new_index = max_val - numeric_value;
            LongWritable sort_index = new LongWritable(new_index);

   Text new_val = new Text(real_key);
            context.write(sort_index, new_val);
   }
        }
    }

    public static class Reducer2
            extends Reducer<LongWritable, Text, Text, LongWritable> {

        long max_val = 1000 * 1000;
        public void reduce(LongWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            for (Text val : values) {

                long real_value = max_val - key.get();
                LongWritable result = new LongWritable(real_value);
                context.write(val, result);
            }
        }
    }


    public static void main(String[] args) throws Exception {
        // job 1
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "crime count");
        job1.setJarByClass(CrimeCount.class);
        job1.setMapperClass(Mapper1.class);
  //      job1.setCombinerClass(Reducer1.class);  // req ?
        job1.setReducerClass(Reducer1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
job1.waitForCompletion(true);

        // job 2
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "crime sort");
        job2.setJarByClass(CrimeCount.class);
        job2.setMapperClass(Mapper2.class);
//        job2.setCombinerClass(Reducer2.class);  // req ?
job2.setMapOutputKeyClass(LongWritable.class);
job2.setMapOutputValueClass(Text.class);

        job2.setReducerClass(Reducer2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
}