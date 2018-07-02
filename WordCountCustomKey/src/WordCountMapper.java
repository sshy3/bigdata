import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable,Text,CustomWordKey,IntWritable> {
    CustomWordKey incomingword = new CustomWordKey();
    public void map(LongWritable inpukey, Text inputval, Context context) throws IOException, InterruptedException {

        String splits[]= inputval.toString().split("//W");
        for(String word: splits){
            if(word.length()>0){

                incomingword.setWord(new Text(word));
                context.write(incomingword, new IntWritable(1));
            }


        }
    }
}
