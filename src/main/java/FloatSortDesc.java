import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FloatSortDesc extends WritableComparator {

    protected FloatSortDesc()
    {
        super(FloatWritable.class, true);
    }

    @SuppressWarnings("rawtypes")

    @Override
    public int compare(WritableComparable value1, WritableComparable value2) {
        FloatWritable float_value1 = (FloatWritable)value1;
        FloatWritable float_value2 = (FloatWritable)value2;

        return -1 * float_value1.compareTo(float_value2);
    }
}
