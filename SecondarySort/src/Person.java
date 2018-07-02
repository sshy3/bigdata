import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Person implements WritableComparable<Person>{
    private Text firstname;

    public Text getFirstname() {
        return firstname;
    }

    public void setFirstname(Text firstname) {
        this.firstname = firstname;
    }

    public Text getLastname() {
        return lastname;
    }

    public void setLastname(Text lastname) {
        this.lastname = lastname;
    }

    private Text lastname;
    public Person(){
    }
    public Person(String firstname, String lastname){
        this.firstname.set(firstname);
        this.lastname.set(lastname);
    }
    @Override
    public int compareTo(Person o) {
        return firstname.compareTo(o.getFirstname());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        firstname.write(dataOutput);
        lastname.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        firstname.readFields(dataInput);
        lastname.readFields(dataInput);
    }
}
