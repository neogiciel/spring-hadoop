package com.neogiciel.spring_hadoop;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.sql.SparkSession;

import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*****************************************************************************************
 * Forwarder le port 9000 afin que le nodename soit accessible
 *  kubectl port-forward -n data service/hadoop-hadoop-hdfs-nn 9000:9000
 *  l'access au serveur se fait hdfs://hadoop-hadoop-hdfs-nn.data.svc.cluster.local:9000
 *  kubectl port-forward -n data service/master 7077:7077
 ******************************************************************************************/
@Service
public class SparkService {

    /*Logger */
    static Logger LOGGER = LoggerFactory.getLogger(SparkService.class);

    /*value*/
    @Value("${spring.hadoop.fs.defaultFS:fs.defaultFS}")
    private String hadoopFS;
        
    @Value("${spring.hadoop.fs.uri:fs.defaultFS:hdfs://localhost:9000}")
    private String hadoopUri;

    
    /*
     * count(String fileName)
     */
    public String count(String fileName) {
        
        LOGGER.info("[SparkService/count] fileName = "+ fileName);
        LOGGER.info("[SparkService/count] hadoopFS = "+ hadoopFS);
        LOGGER.info("[SparkService/count] hadoopUri = "+ hadoopUri);
        try{
            //Path path = new Path(fileName);     
            Path path = new Path("hdfs://hadoop-hadoop-hdfs-nn.data.svc.cluster.local:9000/user/rating.txt");     
            //Path path = new Path("hdfs://localhost:9000/user/rating.txt");     
            LOGGER.info("[SparkService/count] setAppName");
            SparkConf sparkConf = new SparkConf().setAppName("count").set("spark.ui.port","8888").setMaster("spark://192.168.1.12:7077");
           
            LOGGER.info("[SparkService/count] JavaSparkContext");
            JavaSparkContext sc = new JavaSparkContext(sparkConf);
            LOGGER.info("[SparkService/count] textFile");
            JavaRDD<String> lines = sc.textFile(path.toString());
            LOGGER.info("[SparkService/count] Lines count: " + lines.count());
            return "Nombre de lignes = "+lines.count();

        }catch(Exception e){
            LOGGER.info("[SparkService/count] erreur = "+ e.getMessage());
            return "Erreur= "+e.getMessage();
        }
    }
    
    /*
     * count(String fileName)
     */
    public String test(String fileName) {
        
        LOGGER.info("[SparkService/test] fileName = "+ fileName);
        LOGGER.info("[SparkService/test] hadoopFS = "+ hadoopFS);
        LOGGER.info("[SparkService/test] hadoopUri = "+ hadoopUri);
        try{
           
           
            //-----
           //Configuration conf =new Configuration();
            //conf.set(hadoopFS, hadoopUri);
            //FileSystem fileSystem=FileSystem.get(conf);
            //----------
           
            //Path path = new Path(fileName);     
            Path path = new Path("hdfs://hadoop-hadoop-hdfs-nn.data.svc.cluster.local:9000/user/rating.txt");     
            //Path path = new Path("hdfs://localhost:9000/user/rating.txt");     
            LOGGER.info("[SparkService/count] setAppName");
            //SparkConf sparkConf = new SparkConf().setAppName("Workshop").setMaster("local[*]");
            //SparkConf sparkConf = new SparkConf().setAppName("count").setMaster("spark://master.data.svc.cluster.local:7077");
            //SparkConf sparkConf = new SparkConf().setAppName("count").setMaster("spark://master:7077");
            //SparkConf sparkConf = new SparkConf().setAppName("count").setMaster("spark://192.168.1.12:32143");
            //SparkConf sparkConf = new SparkConf().setAppName("count").setMaster("spark://localhost:7077");
            //SparkConf sparkConf = new SparkConf().setAppName("count").set("spark.ui.port","8888").setMaster("spark://192.168.1.12:7077");
            //SparkConf sparkConf = new SparkConf().setAppName("count").set("spark.ui.url","8787").set("spark.ui.port","8787")
            //setMaster("spark://192.168.1.12:7077");

            LOGGER.info("[SparkService/count] Spark Conf");

            SparkConf conf = new SparkConf();
            conf.setAppName("count.words.in.file")
                .setMaster("k8s://https://kubernetes.default.svc:443")
                //.setJars(new String[]{"/app/spring-hadoop.jar"})
                .setJars(new String[]{"/spring-hadoop.jar"})
                .set("spark.driver.host", "10.43.210.228")
                //.set("spark.driver.port", "8080")
                .set("spark.driver.port", "7077")
                .set("spark.kubernetes.namespace", "data")
                .set("spark.kubernetes.container.image", "spark:3.3.2h.1")
                .set("spark.executor.cores", "2")
                .set("spark.executor.memory", "1g")
                .set("spark.kubernetes.authenticate.executor.serviceAccountName", "spark")
                .set("spark.kubernetes.dynamicAllocation.deleteGracePeriod", "20")
                .set("spark.cores.max", "4")
                .set("spark.executor.instances", "2");

            LOGGER.info("[SparkService/count] Spark Session");
            
            /*SparkSession spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();*/

            JavaSparkContext sc = new JavaSparkContext(conf);
            LOGGER.info("[SparkService/count] textFile");
            JavaRDD<String> lines = sc.textFile(path.toString());
            LOGGER.info("[SparkService/count] Lines count: " + lines.count());
            return "Nombre de lignes = "+lines.count();

        }catch(Exception e){
            LOGGER.info("[SparkService/test] erreur = "+ e.getMessage());
            return "Erreur= "+e.getMessage();
        }
    }

    

      
    
}

