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

public class MapReduce3 {
    private static final Logger LOG = Logger.getLogger(MapReduce3.class);


    public static int run(String[] args) throws Exception {
        // args
        // 2 -> output2 path
        // 3 -> output3 path

        Job job = Job.getInstance(MapReduceKNN.knnConf, "job3");
        job.setJarByClass(MapReduce3.class);

        FileInputFormat.addInputPath(job, new Path(args[2]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        job.setMapperClass(Map3.class);
        job.setReducerClass(Reduce3.class);
        job.setNumReduceTasks(Integer.parseInt(args[8]));
        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(Text.class);



        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class Map3 extends Mapper<LongWritable, Text, Text, Text> {
        private String input;
        private int spaceSize;
        private int n;
        private int interval;
        private Map<String, String> mergeInfo;


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
            if (!line.equals("")){
                // id, x, y, cell_id, kNN_list
                String[] array = line.split("\t");
                int x = Integer.parseInt(array[1]);
                int y = Integer.parseInt(array[2]);
                String cellId = array[3];
                List<Double> knnList = Utils.stringToDoubleList(array[4]);
                // draw the boundary circle to find overlapped cells
                double maxDistance = knnList.get(knnList.size() - 1);
                int minX = (int) Math.max(0, x - maxDistance);
                int maxX = (int) Math.min(this.spaceSize, x + maxDistance);
                int minY = (int) Math.max(0, y - maxDistance);
                int maxY = (int) Math.min(this.spaceSize, y + maxDistance);
                Set<String> cellSet = new HashSet<>();
                for (int i = minX; i < maxX + this.interval; i += this.interval){
                    for (int j = minY; j < maxY + this.interval; j += this.interval){
                        cellSet.add(Utils.getCellId(i, j, this.spaceSize, this.n));
                    }
                }
                Set<String> filteredCellSet = new HashSet<>();
                for (String _cellId : cellSet){
                    if (this.mergeInfo.containsKey(_cellId))
                        filteredCellSet.add(this.mergeInfo.get(_cellId));
                }
                // one cell means the circle does not overlap with other cells
                if (filteredCellSet.size() == 1){
                    context.write(new Text(cellId), new Text(array[0] + "\t" + array[1] + "\t" + array[2]
                            + "\t" + array[4] + "\t" + "true"));
                }else{
                    for (String _cellId : filteredCellSet){
                        if (!_cellId.equals(cellId)){
                            context.write(new Text(_cellId), new Text(array[0] + "\t" + array[1] + "\t" + array[2]
                                    + "\t" + array[4] + "\t" + "false"));
                        }
                    }
                }
                context.write(new Text(cellId), new Text(array[0] + "\t" + array[1] + "\t" + array[2]));


            }
        }
    }

    public static class Reduce3 extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text cellId, Iterable<Text> lines, Context context)
                throws IOException, InterruptedException {
            List<String> incorrectLines = new ArrayList<>();
            List<String> correctPoints = new ArrayList<>();
            List<Utils.Point> currentCellPoints = new ArrayList<>();
            // classify the inputs
            for (Text line : lines){
                String _line = line.toString();
                String[] array = _line.split("\t");
                if (array[array.length - 1].equals("false")){
                    incorrectLines.add(_line);
                }else if (array[array.length - 1].equals("true")){
                    correctPoints.add(_line);
                }else{
                    currentCellPoints.add(new Utils.Point(array[0], Integer.parseInt(array[1]), Integer.parseInt(array[2])));
                }
            }
            // for each 'false' cells, compute the knn list in that cell
            for (String line : incorrectLines){
                String[] array = line.split("\t");
                Utils.Point p = new Utils.Point(array[0], Integer.parseInt(array[1]), Integer.parseInt(array[2]));
                List<Double> knnList = Utils.stringToDoubleList(array[3]);
                for (Utils.Point _p : currentCellPoints){
                    knnList.add(p.computeDistance(_p));
                }
                knnList = knnList.subList(0, context.getConfiguration().getInt("k", 0));
                context.write(new Text(p.id), new Text(array[1] + "\t" + array[2] + "\t" + cellId.toString()
                        + "\t" + Utils.doubleListToString(knnList)));
            }
            for (String line : correctPoints){
                String[] array = line.split("\t");
                context.write(new Text(array[0]), new Text(array[1] + "\t" + array[2] + "\t" + cellId.toString()
                        + "\t" + array[3]));
            }

        }

    }

}
