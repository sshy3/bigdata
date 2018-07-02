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


public class SearchWordCountDriver extends Configured implements Tool {

    public static void main(String args[]) throws Exception {
        ToolRunner.run(new SearchWordCountDriver(),args);
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf= new Configuration();
        Job job = Job.getInstance(conf,"Search Word Count Driver");
        job.setMapperClass(SearchWordCountMapper.class);
        job.setReducerClass(SearchWordCountReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job,new Path(strings[0]));
        FileOutputFormat.setOutputPath(job,new Path(strings[1]));
        System.exit(job.waitForCompletion(true)?1:0);
        return 1;
    }
}
