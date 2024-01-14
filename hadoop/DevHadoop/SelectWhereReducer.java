import java.io.IOException;
import java.util.ArrayList;
import java.util.stream.Stream;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

public class SelectWhereReducer extends Reducer<LongWritable, Text,LongWritable, Text>{
//Select distinct * from Employees where type_salaire='Hourly' and prix_houre>55
    public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException,InterruptedException {
        for (Text value:values){
            String[] attributes=value.toString().split(",");
            if (attributes[5].equals("Hourly") && Double.parseDouble(attributes[8])>55)
                context.write(key,value);
        }
    }
}
