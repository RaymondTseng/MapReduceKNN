import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Merge {
    public Map<String, String>  cellMerging(String inputPath, int k, int n) throws IOException {
        Map<String, Integer> cellInfo = new HashMap<String, Integer>();
        Map<String, String> mergeInfo = new HashMap<>();
        Map<String, String> outputMergeInfo = new HashMap<>();
        File temp = new File(inputPath);
        File[] listFiles = temp.listFiles();
        for (File file : listFiles) {
            if (!file.getName().contains("_SUCCESS")) {
                BufferedReader br = new BufferedReader(new FileReader(file));
                String line = null;
                while ((line = br.readLine()) != null) {
                    String[] array = line.split("\t");
                    if (array.length == 1)
                        break;
                    cellInfo.put(array[0], cellInfo.getOrDefault(array[0], 0) + Integer.parseInt(array[1]));
                    mergeInfo.put(array[0], array[0]);
                    //cellInfo.put(array[0], Integer.parseInt(array[1]));
                }
            }
        }
        int keyLength = n;
        while (keyLength > 0) {
            List<String> keyList = new ArrayList<String>(cellInfo.keySet());
            for (String key : keyList) {
                if (key.length() == keyLength){
                    if (cellInfo.getOrDefault(key, 0) < k) {
                        mergeInfo.put(key, key.substring(0, key.length() - 1));
                        for (int i = 0; i < 4; i++){
                            String _key = key.substring(0, key.length() - 1) + String.valueOf(i);
                            mergeInfo.put(_key, key.substring(0, key.length() - 1));
                        }
                    }
                    cellInfo.put(key.substring(0, key.length() - 1),
                            cellInfo.getOrDefault(key.substring(0, key.length() - 1), 0) +
                                    cellInfo.getOrDefault(key, 0));
                }
            }
            System.out.println("finished"+String.valueOf(keyLength));
            keyLength--;
        }

        for (Map.Entry<String,String> entry : mergeInfo.entrySet()){
            if (entry.getKey().length() == n){
                outputMergeInfo.put(entry.getKey(), entry.getValue());
            }
        }
        return outputMergeInfo;

    }

    public static void main(String[] args) throws Exception{
        new Merge().cellMerging(args[0], Integer.parseInt(args[1]), Integer.parseInt(args[2]));
    }



}
