import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/*
    these inputs, must match the ones from the mapper class
 */

public class NetFlixTopTenMoviesReducer extends Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable> {

    private FloatWritable result = new FloatWritable();
    private float sum;
    private int count;


    public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException
    {
        /*
            The format is now this (this example is not our data)

                ("dobbs", [20, 22])
                ("doctor", [545525, 668666])
        */
        sum = 0;
        count = 0;
        // So loop all values and all them up, also keep count for avg division
        for (FloatWritable value : values) {
            sum += value.get();
            count++;
        }

        // write result to file
        context.write(key, result);
    }

}
