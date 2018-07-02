import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomPartitioner extends Partitioner<Text,Text> {

    @Override
    public int getPartition(Text inputkey, Text inputval, int numofreducers) {
        int partitoinnum=0;
        char firstchar = inputval.toString().charAt(0);
        switch (firstchar){
            case 'a':
            case 'e':
            case 'i':
            case 'o':
            case 'u':partitoinnum=0;
                    break;
            default:partitoinnum=1;
        }
        return partitoinnum;
    }
}
