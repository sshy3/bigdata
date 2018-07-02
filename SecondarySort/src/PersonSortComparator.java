import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;

public class PersonSortComparator extends WritableComparator {
    private static final Text.Comparator TEXT_COMPARATOR =  new Text.Comparator();

    protected PersonSortComparator(){
        super(Person.class);
    }
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2){


    }
}
