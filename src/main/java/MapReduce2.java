import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;



public class MapReduce2 {
    private static final Logger LOG = Logger.getLogger(MapReduce2.class);


    public static int run(String[] args) throws Exception {
        // args
        // 0 -> original input path
        // 2 -> output path

        Job job = Job.getInstance(MapReduceKNN.knnConf, "job2");
        job.setJarByClass(MapReduce2.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setMapperClass(Map2.class);
        job.setReducerClass(Reduce2.class);
        job.setNumReduceTasks(Integer.parseInt(args[8]));
        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(Text.class);



        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
        private Map<String, String> mergeInfo;
        private String input;
        private int spaceSize;
        private int n;
        private int interval;


        protected void setup(Mapper.Context context) {
            if (context.getInputSplit() instanceof FileSplit) {
                this.input = ((FileSplit) context.getInputSplit()).getPath().toString();
            } else {
                this.input = context.getInputSplit().toString();
            }
            Configuration config = context.getConfiguration();
            this.mergeInfo = (Map<String, String>) Utils.deserialize(config.get("mergeInfo"));
            this.spaceSize = config.getInt("size", 0);
            this.n = config.getInt("n", 0);
            if (this.n < 0)
                this.n = 0;
            this.interval = this.spaceSize / (int) Math.pow(2, this.n);
        }

        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {
            String line = lineText.toString();
            Text currentCellId = new Text();
            if (!line.equals("")){
                String[] wordArray = line.split("\t");
                String pointId = wordArray[0];
                int x = Integer.parseInt(wordArray[1]);
                int y = Integer.parseInt(wordArray[2]);
                String cellId = Utils.getCellId(x, y, this.spaceSize, n);
                currentCellId = new Text(this.mergeInfo.getOrDefault(cellId, cellId));
                context.write(currentCellId, new Text(pointId + "\t" + wordArray[1] + "\t" + wordArray[2]));

            }
        }
    }

    public static class Reduce2 extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text cellId, Iterable<Text> lines, Context context)
                throws IOException, InterruptedException {

            List<Utils.Point> points = new ArrayList<>();
            for (Text line : lines) {
                String[] array = line.toString().split("\t");
                points.add(new Utils.Point(array[0], Integer.parseInt(array[1]), Integer.parseInt(array[2])));
            }

            if (points.size() == 0)
                return;
            List<List<Double>> knnLists = Utils.computeKNNList(points, context.getConfiguration().getInt("k", 0));
            for (int i = 0; i < points.size(); i++){
                Utils.Point point = points.get(i);
                List<Double> knnList = knnLists.get(i);
                context.write(new Text(point.id), new Text(String.valueOf(point.x) + "\t" +
                        String.valueOf(point.y) + "\t" + cellId.toString() + "\t" +
                        Utils.doubleListToString(knnList)));
            }

        }
    }

}