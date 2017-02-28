
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class IntSortDesc extends WritableComparator {

    protected IntSortDesc()
    {
        super(IntWritable.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable value1, WritableComparable value2) {
        IntWritable int_value1 = (IntWritable)value1;
        IntWritable int_value2 = (IntWritable)value2;

        return -1 * int_value1.compareTo(int_value2);
    }
}
