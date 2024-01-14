import java.io.IOException;
import java.util.ArrayList;
import java.util.stream.Stream;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

public class SelectUnionReducer extends Reducer<LongWritable, Text,LongWritable, Text>{
    //(Select distinct * from Employees) UNION (Select distinct * from Employees)
    private LongWritable count=new LongWritable(0);
    private ArrayList<String> arrays= new ArrayList<>();
    public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException,InterruptedException {
        for (Text value:values){
            if(arrays.stream().noneMatch(s -> s.equals(value.toString()))){
//                String[] att=value.toString().split(",");
                arrays.add(value.toString());
                context.write(count,value);
                count.set(count.get()+1);
            }
        }
    }
}
