import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class NetFlixTopTenUsersMapperSorter extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

    private IntWritable new_key = new IntWritable();
    private IntWritable new_value = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] output =  value.toString().split("\t");

        System.out.print("VALUE((((((((((((((((((((((((((((((((((((" + value);

        new_key.set(Integer.parseInt(output[1]));
        new_value.set(Integer.parseInt(output[0]));
        context.write(new_key, new_value);
    }
}




