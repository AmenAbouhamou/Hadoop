import java.awt.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.stream.Stream;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

public class SelectMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    private LongWritable count=new LongWritable(0);
    private ArrayList<String> arrays= new ArrayList<>();
    public void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException {
            if(arrays.stream().noneMatch(s -> s.equals(value.toString()))){
                arrays.add(value.toString());
                context.write(count,value);
                count.set(count.get()+1);
            }
    }
}
