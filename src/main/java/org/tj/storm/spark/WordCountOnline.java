package org.tj.storm.spark;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
/**
 * 1、local的模拟线程数必须大于等于2 因为一条线程被receiver(接受数据的线程)占用，另外一个线程是job执行
 * 2、Durations时间的设置，就是我们能接受的延迟度，这个我们需要根据集群的资源情况以及监控每一个job的执行时间来调节出最佳时间。
 * 3、 创建JavaStreamingContext有两种方式 （sparkconf、sparkcontext）
 * 4、业务逻辑完成后，需要有一个output operator
 * 5、JavaStreamingContext.start()straming框架启动之后是不能在次添加业务逻辑
 * 6、JavaStreamingContext.stop()无参的stop方法会将sparkContext一同关闭，stop(false) ,默认为true，会一同关闭
 * 7、JavaStreamingContext.stop()停止之后是不能在调用start
 */

/**
 * foreachRDD  算子注意：
 * 1.foreachRDD是DStream中output operator类算子
 * 2.foreachRDD可以遍历得到DStream中的RDD，可以在这个算子内对RDD使用RDD的Transformation类算子进行转化，但是一定要使用rdd的Action类算子触发执行。
 * 3.foreachRDD可以得到DStream中的RDD，在这个算子内，RDD算子外执行的代码是在Driver端执行的，RDD算子内的代码是在Executor中执行。
 */
public class WordCountOnline {


    private static String appName = "spark.streaming.demo";
    private static String master = "local[*]";
    private static String host = "localhost";
    private static int port = 9999;

    public static void main(String[] args) {
        //初始化sparkConf
        SparkConf sparkConf = new SparkConf().setMaster( master ).setAppName( appName );

        //获得JavaStreamingContext
        JavaStreamingContext ssc = new JavaStreamingContext( sparkConf, Durations.seconds( 3 ) );

        //从socket源获取数据
        JavaReceiverInputDStream<String> lines = ssc.socketTextStream( host, port );

        //拆分行成单词
        JavaDStream<String> words = lines.flatMap( new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList( s.split( " " ) ).iterator();
            }
        } );

        //计算每个单词出现的个数
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair( new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>( s, 1 );
            }
        } ).reduceByKey( new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        } );

        //输出结果
        wordCounts.print();

        //开始作业
        ssc.start();
        try {
            ssc.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            ssc.close();
        }
    }
}
