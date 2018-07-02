import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;

public class AvgWordLengthDriver extends Configured implements Tool {
    public static void main(String args[]) throws Exception {
        ToolRunner.run(new AvgWordLengthDriver(),args);
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf=new Configuration();
        Job job= Job.getInstance(conf,"Word Avg Length");
        job.setMapperClass(AvgWordLengthMapper.class);
        job.setReducerClass(AvgWordLengthReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputValueClass(FloatWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job,new Path(strings[0]));
        FileOutputFormat.setOutputPath(job,new Path(strings[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }
}
