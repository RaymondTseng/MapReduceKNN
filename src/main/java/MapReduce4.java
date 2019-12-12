import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

public class MapReduce4 {
    private static final Logger LOG = Logger.getLogger(MapReduce4.class);


    public static int run(String[] args) throws Exception {
        // args
        // 3 -> output3 path
        // 4 -> output4 path

        Job job = Job.getInstance(MapReduceKNN.knnConf, "job4");
        job.setJarByClass(MapReduce3.class);

        FileInputFormat.addInputPath(job, new Path(args[3]));
        FileOutputFormat.setOutputPath(job, new Path(args[4]));
        job.setMapperClass(Map4.class);
        job.setReducerClass(Reduce4.class);
        job.setOutputKeyClass(Text.class);
        job.setNumReduceTasks(Integer.parseInt(args[8]));
        job.setOutputValueClass(Text.class);



        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class Map4 extends Mapper<LongWritable, Text, Text, Text> {
        private String input;


        protected void setup(Mapper.Context context) {
            if (context.getInputSplit() instanceof FileSplit) {
                this.input = ((FileSplit) context.getInputSplit()).getPath().toString();
            } else {
                this.input = context.getInputSplit().toString();
            }
            Configuration config = context.getConfiguration();


        }

        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {
            String line = lineText.toString();
            if (!line.equals("")){
                // id, x, y, cell_id, kNN_list
                String[] array = line.split("\t");
                context.write(new Text(array[0]), new Text(array[4]));

            }
        }
    }

    public static class Reduce4 extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text id, Iterable<Text> lines, Context context)
                throws IOException, InterruptedException {
            // integrate all knn list
            List<Double> integratedKnnList = new ArrayList<>();
            for (Text line : lines){
                List<Double> knnList = Utils.stringToDoubleList(line.toString());
                integratedKnnList.addAll(knnList);
            }
            context.write(id, new Text(Utils.doubleListToString(integratedKnnList.subList(0,
                    context.getConfiguration().getInt("k", 0)))));
        }

    }
}
