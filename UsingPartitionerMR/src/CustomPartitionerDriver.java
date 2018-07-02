import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CustomPartitionerDriver extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf =new Configuration();
        Job job= Job.getInstance(conf,"USING PARTIONER");
        job.setMapperClass(CustomePartitionerMapper.class);
        job.setReducerClass(CustomPartitionerReducer.class);
        job.setPartitionerClass(CustomPartitioner.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        System.exit(job.waitForCompletion(true)?1:0);
        return 1;

    }
    public static void main(String args[]) throws Exception {

        ToolRunner.run(new CustomPartitionerDriver(),args);
    }
}
