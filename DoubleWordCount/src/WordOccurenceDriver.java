import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordOccurenceDriver extends Configured implements Tool{

    public static void main(String args[]) throws Exception {
        ToolRunner.run(new WordOccurenceDriver(),args);
    }
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf=new Configuration();
        Job job=Job.getInstance(conf,"Word Occurence");
        job.setMapperClass(WordOccurenceWrapper.class);
        job.setReducerClass(WordOccurenceReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setNumReduceTasks(3);

        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;

    }
}
