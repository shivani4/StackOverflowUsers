package project.org;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.Text;

public class DescendingIntComparator extends WritableComparator {

        protected DescendingIntComparator() {
        super(Text.class, true);
    }
    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        Text key1 = (Text) w1;
        Text key2 = (Text) w2;
        Integer keys1=Integer.parseInt(key1.toString());
        Integer keys2=(Integer.parseInt(key2.toString()));
        return -1 * keys1.compareTo(keys2);
    }
}

