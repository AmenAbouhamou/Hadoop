import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;

import java.util.Arrays;
import java.util.InputMismatchException;
import java.util.Scanner;

public class Select {
    private static int oper;
    public static void main(String[] args) throws Exception {
        // on verifie que les chemins des données et resultats sont fournis
        if (args.length != 2) {
            System.err.println("Syntaxe : Select <input path> <output path>");
            System.exit(-1);
        }
        System.out.println("Selectionner la l'operateur de l'algebre relationelle ");
        System.out.println("1) Select distinct * from Employees ");
        System.out.println("2) Select distinct * from Employees where type_salaire='Hourly' and prix_heure>55 ");
        System.out.println("3) (Select distinct * from Employees) UNION (Select distinct * from Employees1) ");
        System.out.println("4) (Select distinct * from Employees) INTERSECTION (Select distinct * from Employees1) ");
        System.out.print(">> ");
        try {
            oper=new Scanner(System.in).nextInt();
        }catch(InputMismatchException e){
            oper=1;
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "select_inst");

        job.setJarByClass(Select.class);
        job.setJobName("select");

        // on connecte les entrée et sorties sur les répertoires passés en paramètre
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        switch (oper){
            case 1:
                job.setMapperClass(SelectMapper.class);// définir la classe qui réalise le map
                job.setReducerClass(SelectReducer.class);// definir la classe qui réalise le reduce
                break;
            case 2:
                job.setMapperClass(SelectMapper.class);// définir la classe qui réalise le map
                job.setReducerClass(SelectWhereReducer.class);// definir la classe qui réalise le reduce
                break;
            case 3:
                job.setMapperClass(SelectMapper.class);// définir la classe qui réalise le map
                job.setReducerClass(SelectUnionReducer.class);// definir la classe qui réalise le reduce
                break;
            case 4:
                job.setMapperClass(SelectMapper.class);// définir la classe qui réalise le map
                job.setReducerClass(SelectIntersectReducer.class);// definir la classe qui réalise le reduce
                break;
        }


        // definir le format de la sortie
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        // lancer l'exec
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
