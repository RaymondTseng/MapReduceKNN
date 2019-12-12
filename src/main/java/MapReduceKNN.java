import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.util.Map;

public class MapReduceKNN {
    public static final Configuration knnConf = new Configuration();
    public static void main(String[] args) throws Exception{
        // 0 -> input path
        // 1 -> output1 path
        // 2 -> output2 path
        // 3 -> output3 path
        // 4 -> output4 path
        // 5 -> size the range of data distribution
        // 6 -> n decompostion
        // 7 -> k k neighbours
        // 8 -> r number of reduce task
        knnConf.setInt("size", Integer.parseInt(args[5]));
        knnConf.setInt("n", Integer.parseInt(args[6]));
        knnConf.setInt("k", Integer.parseInt(args[7]));
        // For copy the output1 from hdfs to local
        knnConf.set("fs.defaultFS", "hdfs://ric-master-01.sci.pitt.edu:8020");
        MapReduce1.run(args);
        FileSystem hdfsFileSystem = FileSystem.get(knnConf);
        FileSystem localFileSystem = FileSystem.getLocal(new Configuration());
        FileUtil.copyMerge(hdfsFileSystem, new Path("/user/ziz54/knn/output1/"), localFileSystem, new Path("./output1/output"), false, knnConf, null);
        Map<String, String> mergeInfo = new Merge().cellMerging(
                "./output1/", knnConf.getInt("k", 0) + 1, knnConf.getInt("n", 0));
        knnConf.set("mergeInfo", Utils.serialize(mergeInfo));
        MapReduce2.run(args);
        MapReduce3.run(args);
        MapReduce4.run(args);
    }
}
