import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

public class SelectWhereMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    private LongWritable count;
    private ArrayList<String> arrays;
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        count=new LongWritable(0);
        arrays= new ArrayList<String>();
    }
    public void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException {
        String[] attributes=value.toString().split(",");
        if(arrays.stream().noneMatch(s -> s.equals(value.toString())) && attributes[5].equals("Hourly") && Double.parseDouble(attributes[8])>55){
            arrays.add(value.toString());
            context.write(count,value);
        }
        count.set(count.get()+1);
    }
}
