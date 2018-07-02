import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class InvertedIndexReducer extends Reducer<Text,Text,Text,Text>{
    Text fileList=new Text();
    public void reduce(Text key, Iterable<Text>list,Context context) throws IOException, InterruptedException {
       String result="";
       for(Text filename:list){
           if(!result.contains(filename.toString())){
               result=result+filename+",";
           }
       }
       result=result.substring(0,result.length()-1);
       context.write(key,new Text(result));
    }
}
