import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class NetFlixTopTenUsersMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text new_key = new Text();
    private IntWritable increment = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] output =  value.toString().split(",");
        new_key.set(output[1]);

        context.write(new_key, increment);
    }
}




