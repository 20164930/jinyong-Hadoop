import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Mark_tra {
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            int index_t = line.indexOf("\t");
            int index_j = line.indexOf("#");
            int index_dollar = line.indexOf("$");
            String PR = line.substring(index_t + 1, index_j);
            String name = line.substring(index_dollar + 1, index_t);
            String nameList = line.split("#")[1];
            String label = line.substring(0, index_dollar);
            String[] names = nameList.split(";");
            for (int i = 0; i < names.length; i++) {
                context.write(new Text(names[i].split(":")[0]), new Text(label + "#" + name)); //(武敦儒，1#一灯大师)
            }
            context.write(new Text(name), new Text("#" + nameList)); //（一灯大师,#武敦儒:0.0055248618784530384;...）
            context.write(new Text(name), new Text("$" + label)); // （一灯大师，$1）
            context.write(new Text(name), new Text("@" + PR)); //（一灯大师，@0.20358545173779）
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        HashMap<String, String> name_label_map = new HashMap<>();

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String label = "";
            String nameList = "";
            String pr = "";
            HashMap<String, String> relation_name_label = new HashMap<>();
            for (Text text : values) {
                String str = text.toString();
                if (str.length() > 0 && str.charAt(0) == '$') {
                    label = str.replace("$", "");
                } else if (str.length() > 0 && str.charAt(0) == '@') {
                    pr = str.replace("@", "");
                } else if (str.length() > 0 && str.charAt(0) == '#') {
                    nameList = str.replace("#", "");
                } else if (str.length() > 0) {
                    String[] element = str.split("#");
                    relation_name_label.put(element[1], element[0]); //(一灯大师，1)
                }
            }

            HashMap<String, Float> label_pr_map = new HashMap<>();
            StringTokenizer nameList_Tokenizer = new StringTokenizer(nameList, ";");
            while (nameList_Tokenizer.hasMoreTokens()) {
                String[] name_pr = nameList_Tokenizer.nextToken().split(":");//取出namelist所有人
                Float current_pr = Float.parseFloat(name_pr[1]); //和key的关系紧密度
                String current_label = relation_name_label.get(name_pr[0]); //此人当前的标签
                Float label_pr;
                if ((label_pr = label_pr_map.get(current_label)) != null) {  //这个标签已经在hashmap里
                    label_pr_map.put(current_label, label_pr + current_pr);  //累加相关度，因为可能有多个不同的人属于同一个标签
                } else {
                    label_pr_map.put(current_label, current_pr);
                }
            }

            //找到最大相关度的人的标签
            StringTokenizer tokenizer = new StringTokenizer(nameList, ";");
            float maxPr = Float.MIN_VALUE;
            List<String> maxNameList = new ArrayList<>();
            while (tokenizer.hasMoreTokens()) {
                String[] element = tokenizer.nextToken().split(":");
                float tmpPr = label_pr_map.get(relation_name_label.get(element[0]));
                if (maxPr < tmpPr) {
                    maxNameList.clear();
                    maxPr = tmpPr;
                    maxNameList.add(element[0]);
                } else if (maxPr == tmpPr) {
                    maxNameList.add(element[0]);
                }
            }

            Random random = new Random();
            int index = random.nextInt(maxNameList.size());
            String target_name = maxNameList.get(index);
            String target_label = relation_name_label.get(target_name);
            if (name_label_map.get(target_name) != null) {
                target_label = name_label_map.get(target_name);  //之所以将（name,label）存入hashmap是因为，每调用一次reduce方法都会更改某些name的标签，
                                                                 // 但这些内容还没输出，只放在内存中
            } else {
                name_label_map.put(key.toString(), target_label);
            }
            if (target_label == null) {
                System.out.println();
            }
            context.write(new Text(target_label + "$" + key.toString()), new Text(pr + "#" + nameList));
        }
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "F:\\hadoop-2.7.6");
        for (int i = 0; i < 6; i++) {
            Configuration conf1 = new Configuration();
            Job job1 = new Job(conf1);
            job1.setJarByClass(Mark_tra.class);
            job1.setJobName("Mark_tra");
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(Text.class);
            job1.setMapperClass(Map.class);
            job1.setReducerClass(Reduce.class);
            if (i == 0) {
                Path path1 = new Path("hdfs://192.168.88.128:9000/mark");
                FileSystem fileSystem1 = path1.getFileSystem(conf1);
                if (fileSystem1.exists(path1)) {
                    fileSystem1.delete(path1, true);
                }
                FileInputFormat.addInputPath(job1, new Path("hdfs://192.168.88.128:9000/pageRank0/part-r-00000"));
                FileOutputFormat.setOutputPath(job1, new Path("hdfs://192.168.88.128:9000/mark"));
            } else {
                String from, to;
                if (i % 2 == 0) {
                    from = "mark0";
                    to = "mark";
                } else {
                    from = "mark";
                    to = "mark0";
                }
                Path path1 = new Path("hdfs://192.168.88.128:9000/" + to);
                FileSystem fileSystem1 = path1.getFileSystem(conf1);
                if (fileSystem1.exists(path1)) {
                    fileSystem1.delete(path1, true);
                }
                FileInputFormat.addInputPath(job1, new Path("hdfs://192.168.88.128:9000/" + from + "/part-r-00000"));
                FileOutputFormat.setOutputPath(job1, new Path("hdfs://192.168.88.128:9000/" + to));
            }
            job1.waitForCompletion(true);
        }
    }
}

