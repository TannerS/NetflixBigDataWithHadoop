import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class NetFlixTopTenUsersMapper extends Mapper<IntWritable, Text, IntWritable, IntWritable> {

    private IntWritable new_key = new IntWritable();
    private final static IntWritable increment = new IntWritable(1);


    @Override
    protected void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] output =  value.toString().split(",");
        new_key.set(Integer.valueOf(output[1]));
        context.write(new_key, increment);
    }
}




