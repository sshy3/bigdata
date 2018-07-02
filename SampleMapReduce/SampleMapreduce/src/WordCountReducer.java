import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text,Iterable<IntWritable>,Text,IntWritable> {
    private IntWritable finalvalue=new IntWritable();
    public void reduce(Text key,Iterable<IntWritable> list,Context context) throws IOException, InterruptedException {

        int sum=0;
        for(IntWritable val:list){
            sum=sum+val.get();
            finalvalue.set(sum);
            context.write(key,finalvalue);
        }
    }
}
