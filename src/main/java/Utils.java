import java.util.*;
import java.io.*;
import java.math.*;

public class Utils {
    public static String serialize(Object object) {
        ObjectOutputStream oos = null;
        ByteArrayOutputStream baos = null;
        try {
            baos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(baos);
            oos.writeObject(object);
            return Base64.getEncoder().encodeToString(baos.toByteArray());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            close(oos);
            close(baos);
        }
        return null;
    }

    public static Object deserialize(String str){
        byte[] bytes = Base64.getDecoder().decode(str);
        ByteArrayInputStream bais = null;
        ObjectInputStream ois = null;
        try {
            bais = new ByteArrayInputStream(bytes);
            ois = new ObjectInputStream(bais);
            return ois.readObject();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            close(bais);
            close(ois);
        }
        return null;
    }

    public static void close(Closeable closeable){
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static List<List<Double>> computeKNNList(List<Point> points, int k){
        List<List<Double>> distances = new ArrayList<>();
        for (int i = 0; i < points.size(); i++){
            Point p = points.get(i);
            List<Double> distance = new ArrayList<>();
            for (int j = 0; j < points.size(); j++){
                if (i != j){
                    distance.add(p.computeDistance(points.get(j)));
                }
            }
            Collections.sort(distance);
            distances.add(distance.subList(0, k));

        }
        return distances;
    }

    public static String doubleListToString(List<Double> list){
        StringBuilder temp = new StringBuilder();
        for (int i = 0; i < list.size(); i++){
            temp.append(String.valueOf(list.get(i))).append(",");
        }

        String res = temp.toString();
        return res.substring(0, res.length() - 1);
    }

    public static List<Double> stringToDoubleList(String str){
        String[] array = str.split(",");
        List<Double> res = new ArrayList<>();
        for (String s : array){
            res.add(Double.valueOf(s));
        }
        return res;
    }

    public static class Point{
        String id;
        int x;
        int y;
        public Point(String id, int x, int y){
            this.id = id;
            this.x = x;
            this.y = y;
        }
        public double computeDistance(Point p){
            return Math.sqrt(Math.pow((this.x - p.x), 2) + Math.pow((this.y - p.y), 2));

        }
    }

    public static String getCellId(int x, int y, int interval, int n){
        int count = 1;
        StringBuilder cellId = new StringBuilder();
        while (count <= n){
            interval = (int) (interval / 2);
            int _x = (int) x / interval;
            int _y = (int) y / interval;
            cellId.append(String.valueOf(_x + _y * 2));
            x -= _x * interval;
            y -= _y * interval;
            count ++;
        }
        return cellId.toString();
    }
}
