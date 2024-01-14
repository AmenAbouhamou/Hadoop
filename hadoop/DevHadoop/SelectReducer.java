import java.io.IOException;
import java.util.ArrayList;
import java.util.stream.Stream;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

public class SelectReducer extends Reducer<LongWritable, Text,LongWritable, Text>{
    public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException,InterruptedException {
//        context.write(key,value);
    for (Text t:values){
        System.out.println("key = " + key + ", values = " + t);
    }
    }
}
