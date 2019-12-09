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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KNN extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(KNN.class);

    public static void main(String[] args) throws Exception{
        int res = ToolRunner.run(new KNN(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        // args
        // 0 - > knn space size
        // 1 - > knn decomposition number
        // 2 -> knn k
        // 3 - > job input path
        // 4 - > job output path
        // 5 - > merge information output path


        Job job1 = Job.getInstance(getConf(), "job");
        job1.setJarByClass(this.getClass());
        // Use TextInputFormat, the default unless job.setInputFormatClass is used
        FileInputFormat.addInputPath(job1, new Path(args[3]));
        FileOutputFormat.setOutputPath(job1, new Path(args[4]));
        job1.getConfiguration().setInt("knn.space.size", Integer.parseInt(args[0]));
        job1.getConfiguration().setInt("knn.n", Integer.parseInt(args[1]));
        job1.setMapperClass(KNN.Map1.class);
        job1.setCombinerClass(KNN.Reduce1.class);
        job1.setReducerClass(KNN.Reduce1.class);
        job1.setNumReduceTasks(1);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);



        return job1.waitForCompletion(true) ? 0 : 1;
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
            this.spaceSize = config.getInt("knn.space.size", 0);
            this.n = config.getInt("knn.n", 0);
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
                int count = 1;
                int interval = this.spaceSize;
                StringBuilder cellId = new StringBuilder();
                while (count <= this.n){
                    interval = (int) (interval / 2);
                    int _x = (int) x / interval;
                    int _y = (int) y / interval;
                    cellId.append(String.valueOf(_x + _y * 2));
                    x -= _x * interval;
                    y -= _y * interval;
                    count ++;
                }
                currentCellId = new Text(cellId.toString());
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



    private void cellMerging(String inputPath, int k, int n, String outputPath) throws IOException {
        Map<String, Integer> cellInfo = new HashMap<String, Integer>();
        Map<String, String> mergeInfo = new HashMap<>();
        File temp = new File(inputPath);
        System.out.println(temp.getAbsolutePath());
        File[] listFiles = temp.listFiles();
        for (File file : listFiles){
            if (!file.getName().contains("_SUCCESS")){
                BufferedReader br = new BufferedReader(new FileReader(file));
                String line = null;
                while ((line = br.readLine()) != null){
                    String[] array = line.split("\t");
                    cellInfo.put(array[0], cellInfo.getOrDefault(array[0], 0) + Integer.parseInt(array[1]));
                    //cellInfo.put(array[0], Integer.parseInt(array[1]));
                }
            }
        }
        QuadTree root = new QuadTree("", n, cellInfo);
        root.travelAndPrune(k, mergeInfo);
        BufferedWriter bw = new BufferedWriter(new FileWriter(outputPath));
        for (Map.Entry<String, String> entry : mergeInfo.entrySet()){
            bw.write(entry.getKey() + "\t" + entry.getValue() + "\n");
        }
        bw.close();



    }

    private class QuadTree{
        public QuadTree parent;
        public List<QuadTree> children;
        public String label;
        public int pointsNum;
        public QuadTree (String label, int n, Map<String, Integer> cellInfo){
            this.label = label;
            if (this.label.length() < n){
                this.children = new ArrayList<>();
                for (int i = 0; i < 4; i++){
                    QuadTree child = new QuadTree(this.label + String.valueOf(i), n, cellInfo);
                    child.parent = this;
                    this.children.add(child);
                    this.pointsNum += child.pointsNum;
                }
            }else{
                this.pointsNum = cellInfo.getOrDefault(this.label, 0);
            }
        }

        public void travelAndPrune(int k, Map<String, String> mergeInfo){
            if (this.pointsNum < k){
                travelAndAdd(this, this.label, mergeInfo);
            }else{
                for (QuadTree child : this.children){
                    child.travelAndPrune(k, mergeInfo);
                }
            }
        }

        private void travelAndAdd(QuadTree node, String parentLabel, Map<String, String> mergeInfo){
            if (node.children.equals(null)){
                mergeInfo.put(node.label, parentLabel);
            }else{
                for (QuadTree child : node.children){
                    travelAndAdd(child, parentLabel, mergeInfo);
                }

            }
        }

    }


}
