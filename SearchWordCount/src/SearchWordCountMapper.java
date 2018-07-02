import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SearchWordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    private String searchword;
    public void map(LongWritable keyin,Text valin,Context context) throws IOException, InterruptedException {

        String splits[]=valin.toString().trim().split("\\W");
        for (String word:splits){
            if(word.equals(searchword)){
                context.write(new Text(word),new IntWritable(1));
            }
        }
    }
    public void setup(Context context){
        searchword = context.getConfiguration().get("search.word"); //-Dsearch.word="word" for using through command line
    }
}
