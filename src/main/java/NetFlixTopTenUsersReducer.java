import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NetFlixTopTenUsersReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();
    private int count;


    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    {
        count = 0;
        // add each value of 1 for each time a user has rated
        for (IntWritable value : values) {
            count += value.get();
        }

        result.set(count);
        context.write(key, result);
    }
}
