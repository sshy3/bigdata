import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AvgWordLengthMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
        String[] split=value.toString().split("\\W");
        for(String word:split){
            if(word.length()>=1) {
                IntWritable len = new IntWritable(word.length());
                Text mapkey = new Text();
                String temp = word.substring(0,1);
                mapkey.set(new Text(temp));
                context.write(new Text(word), len);
            }
        }

    }

}
