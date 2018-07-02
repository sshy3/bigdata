import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    public void map(LongWritable inputKey,Text inputVal, Context context) throws IOException, InterruptedException {
        String line=inputVal.toString();
        String[] splits=line.trim().split("\\W+");
        for(String outputKey:splits){
            context.write(new Text(outputKey),new IntWritable(1));
        }
    }
}
