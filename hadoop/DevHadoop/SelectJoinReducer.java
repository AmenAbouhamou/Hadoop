import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

public class SelectJoinReducer extends Reducer<LongWritable, Text,LongWritable, Text>{
    //(Select distinct * from Employees,Lieu_Travail where Employe.idtravail=Lieu_Travail.id
    private LongWritable count=new LongWritable(0);
    private ArrayList<String> arrays= new ArrayList<>(),
            lieu= new ArrayList<>(),
            user= new ArrayList<>();
    public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException,InterruptedException {
        for (Text value:values){
            String[] att = value.toString().split(",");
            if (arrays.stream().noneMatch(s -> s.equals(value.toString())) && att.length > 2) {
                arrays.add(value.toString());
            }
            if (lieu.stream().noneMatch(s -> s.equals(value.toString())) && att.length == 2) {
                lieu.add(value.toString());
            }
        }
        for (String userval:arrays) {
            for (String lieuval : lieu) {
                if (user.stream().noneMatch(s -> s.equals(userval + "," + lieuval))) {
                    user.add(userval + "," + lieuval);
                    String[] attrib = (userval + "," + lieuval).split(",");
                    if (attrib[3].equals(attrib[9])) {
                        String newVal=joinString(attrib,3,10,9);
                        context.write(count, new Text(newVal));
                        count.set(count.get() + 1);
                    }
                }
            }
        }
    }
    public String joinString(String[] a,int i1,int i2,int limit){
        StringBuilder msg= new StringBuilder();
        for (int i = 0; i < limit-1; i++) {
            if (i!=i1)
                msg.append(a[i]).append(",");
            else
                msg.append(a[i2]).append(",");
        }
        msg.append(a[limit-1]);
        return msg.toString();
    }
}
