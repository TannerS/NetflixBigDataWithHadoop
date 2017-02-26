import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class NetFlixTopTenUsersReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    private IntWritable result = new IntWritable();
    private float sum;
    private int count;


    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    {
        sum = 0;
        count = 0;

        for (IntWritable value : values) {
            sum += value.get();
            count++;
        }

        context.write(key, result);
    }

}
