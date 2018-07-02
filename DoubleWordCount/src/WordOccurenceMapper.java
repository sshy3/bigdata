import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordOccurenceMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    Text key=new Text();
    IntWritable one=new IntWritable(1);
    public void map(LongWritable inputKey, Text inputValue, Context context)throws IOException,InterruptedException{
    String line=inputValue.toString();
    String[] splits=line.split("\\W+");
    for (int i=0;i<splits.length-1;i++){
        key.set(splits[i]+":"+splits[i+1]);
        context.write(key,one);
    }

    }
}
