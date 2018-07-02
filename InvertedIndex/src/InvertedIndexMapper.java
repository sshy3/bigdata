import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class InvertedIndexMapper extends Mapper<LongWritable,Text,Text,Text> {

    public void map(LongWritable inputKey, Text inputvalue, Context context) throws IOException, InterruptedException {
    String line=inputvalue.toString();
    String splits[]=line.split("\\W");
        FileSplit fileSplit= (FileSplit) context.getInputSplit();
        Path path = fileSplit.getPath();

        String fileName=path.getName();
        for(String key:splits) {
            context.write(new Text(key), new Text(fileName));
        }
    }
}
