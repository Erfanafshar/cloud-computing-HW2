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

  public static class ParserMapper
       extends Mapper<Object, Text, Text, LongWritable>{

    private final static LongWritable one = new LongWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
String [] fields = value.toString().split(",");
int arrSize = fields.length;

if (arrSize == 20){
if (fields[12].matches("-?\\d+(\\.\\d+)?"))
                word.set(fields[12]);

            } else if (arrSize == 21){
if (fields[13].matches("-?\\d+(\\.\\d+)?"))
               word.set(fields[13]);

            } else if (arrSize == 22){
if (fields[14].matches("-?\\d+(\\.\\d+)?"))
               word.set(fields[14]);

            } else if (arrSize == 23){
if (fields[12].matches("-?\\d+(\\.\\d+)?"))
                word.set(fields[12]); // should be removed

            } else if (arrSize == 24){
if (fields[12].matches("-?\\d+(\\.\\d+)?"))
                word.set(fields[12]);

            } else if (arrSize == 25){
if (fields[13].matches("-?\\d+(\\.\\d+)?"))
                word.set(fields[13]);

            } else if (arrSize == 26){
if (fields[14].matches("-?\\d+(\\.\\d+)?"))
                word.set(fields[14]);

            } else if (arrSize == 28){
if (fields[16].matches("-?\\d+(\\.\\d+)?"))
                word.set(fields[16]);

            }

        context.write(word, one);
      }
    }

public static class SumReducer
       extends Reducer<Text,LongWritable,Text,LongWritable> {
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

public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "crime count");
    job.setJarByClass(CrimeCount.class);
    job.setMapperClass(ParserMapper.class);
    job.setCombinerClass(SumReducer.class);
    job.setReducerClass(SumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

