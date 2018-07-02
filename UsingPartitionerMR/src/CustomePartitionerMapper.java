import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CustomePartitionerMapper extends Mapper<LongWritable,Text,Text,Text> {
    public void map(LongWritable inputkey, Text inputval,Context context) throws IOException, InterruptedException {

        String line=inputval.toString();
        String splits[]=line.trim().split("\\W");
        for(String outputkey:splits){
            if (outputkey.length()>0){
                context.write(new Text(outputkey.substring(0,1).toLowerCase()),new Text(outputkey));
            }
        }
    }
}
