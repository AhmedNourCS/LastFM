package src;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
/**
 * Created by Venkatram on 7/29/2017.
 */
public class LastFM {

    public class LastFMConstants {
        public static final int USER_ID = 0;
        public static final int TRACK_ID = 1;
        public static final int SHARED = 2;
        public static final int RADIO = 3;
        public static final int SKIPPED = 4;
    }

    public static class UniqueListenerMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        IntWritable trackId = new IntWritable();
        IntWritable userId = new IntWritable();
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException{

            String[] parts = value.toString().split("[|]");
            trackId.set(Integer.parseInt(parts[LastFMConstants.TRACK_ID]));
            userId.set(Integer.parseInt(parts[LastFMConstants.USER_ID]));
            context.write(new Text(trackId.toString()), userId);

        }
    }

    public static class UniqueListenerReduceer extends Reducer<Text, IntWritable, Text, IntWritable>{
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException{

            Set<Integer> userIdSet = new HashSet<>();
            for(IntWritable w: values){
                userIdSet.add(w.get());
            }
            context.write(key, new IntWritable(userIdSet.size()));
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"LastFM");
        job.setJarByClass(LastFM.class);
        job.setMapperClass(UniqueListenerMapper.class);
        job.setReducerClass(UniqueListenerReduceer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        Path outputPath = new Path(args[1]);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        outputPath.getFileSystem(conf).delete(outputPath,true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }


}