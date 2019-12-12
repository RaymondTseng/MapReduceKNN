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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.*;

public class MapReduce1 {

    private static final Logger LOG = Logger.getLogger(MapReduce1.class);


    public static int run(String[] args) throws Exception {
        // args
        // 0 - > job input path
        // 1 - > job output path


        Job job = Job.getInstance(MapReduceKNN.knnConf, "job1");
        job.setJarByClass(MapReduce1.class);
        // Use TextInputFormat, the default unless job.setInputFormatClass is used
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(MapReduce1.Map1.class);
        job.setCombinerClass(MapReduce1.Reduce1.class);
        job.setReducerClass(MapReduce1.Reduce1.class);
        job.setNumReduceTasks(Integer.parseInt(args[8]));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);



        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class Map1 extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private int spaceSize = 0;
        private int n = 0;
        private int interval = 0;
        private String input;


        protected void setup(Mapper.Context context) {
            if (context.getInputSplit() instanceof FileSplit) {
                this.input = ((FileSplit) context.getInputSplit()).getPath().toString();
            } else {
                this.input = context.getInputSplit().toString();
            }
            Configuration config = context.getConfiguration();
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
                int x = Integer.parseInt(wordArray[1]);
                int y = Integer.parseInt(wordArray[2]);
                String cellId = Utils.getCellId(x, y, this.spaceSize, n);
                currentCellId = new Text(cellId);
                context.write(currentCellId, one);

            }
        }
    }

    public static class Reduce1 extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text word, Iterable<IntWritable> counts, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable count : counts) {
                sum += count.get();
            }
            context.write(word, new IntWritable(sum));
        }
    }






}
