import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

public class SelectIntersectReducer extends Reducer<LongWritable, Text,LongWritable, Text>{
    //(Select distinct * from Employees) INTERSECTION (Select distinct * from Employees1)
    private LongWritable count=new LongWritable(0);
    private ArrayList<String> arrays= new ArrayList<>();
    public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException,InterruptedException {
        for (Text value:values){
            if(arrays.stream().anyMatch(s -> s.equals(value.toString()))){
                context.write(count,value);
                count.set(count.get()+1);
            }
            arrays.add(value.toString());
        }
    }
}
