package com.neogiciel.spring_hadoop;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import org.springframework.stereotype.Service;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

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
 *
 ******************************************************************************************/
@Service
public class HdfsService {

    /*Logger */
    static Logger LOGGER = LoggerFactory.getLogger(HdfsService.class);
 
    /*value*/
    @Value("${spring.hadoop.fs.defaultFS:fs.defaultFS}")
    private String hadoopFS;
        
    @Value("${spring.hadoop.fs.uri:fs.defaultFS:hdfs://localhost:9000}")
    private String hadoopUri;

    /*
     * readFile(String fileName)
     */
    public String readFile(String fileName) {
        try {
            //String fileName="/data/people/hello.txt";
            LOGGER.info("[HdfsService/readFile] fileName = "+ fileName);
            LOGGER.info("[HdfsService/readFile] hadoopFS = "+ hadoopFS);
            LOGGER.info("[HdfsService/readFile] hadoopUri = "+ hadoopUri);
            Configuration conf =new Configuration();
            conf.set(hadoopFS, hadoopUri);
            FileSystem fileSystem=FileSystem.get(conf);
            
             
            if(fileSystem.exists(new Path(fileName))) {
                LOGGER.info("[HdfsService/readFile] readFile = File exists!");
                Path path = new Path(fileName);
                FSDataInputStream inputStream = fileSystem.open(path);
                //String out= IOUtils.toString(inputStream, "UTF-8");
                //LOGGER.info("[HdfsService/readFile] out = "+out);
                InputStreamReader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
                BufferedReader bufferedReader = new BufferedReader(reader);
                String line = null; 
                String reponse = "";            
                while ((line=bufferedReader.readLine())!=null){
                    LOGGER.info("[HdfsService/readFile] readFile = "+line);
                    reponse = reponse + "\n" + line;
                }
                inputStream.close();
                fileSystem.close();
                
                LOGGER.info("[HdfsService/readFile] return = "+reponse);
                return reponse;

            }else {
                LOGGER.info("[HdfsService/readFile] readFile = File not exists!");
                return "File not exists!";
            }
            
        }catch(Exception e){
            LOGGER.info("[HdfsService/readFile] erreur = "+ e.getMessage());
            return "Erreur= "+e.getMessage();
        }
    }
    /*
     * writeFile(String fileName,String body)
     */
    public String writeFile(String fileName,String body) {
        try {
            //String fileName="/data/people/hello.txt";
            LOGGER.info("[HdfsService/writeFile] writeFile = "+ fileName);
            LOGGER.info("[HdfsService/writeFile] body = "+ body);
            Configuration conf =new Configuration();
            conf.set(hadoopFS, hadoopUri);
            FileSystem fileSystem=FileSystem.get(conf);

            if(fileSystem.exists(new Path(fileName))) {
                LOGGER.info("[HdfsService/writeFile] writeFile = File exists!");
                Path path = new Path(fileName);
                FSDataOutputStream fsDataOutputStream = fileSystem.append(path);
                BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream,StandardCharsets.UTF_8));
                bufferedWriter.write("Java API to append data in HDFS file");
                bufferedWriter.newLine();
                bufferedWriter.close();
                fileSystem.close();

            
            }else {
                LOGGER.info("[HdfsService/writeFile] writeFile = File not exists!");
                Path path = new Path(fileName);
                FSDataOutputStream fsDataOutputStream = fileSystem.create(path,true);
                
                BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream,StandardCharsets.UTF_8));
                bufferedWriter.write("Java API to write data in HDFS");
                bufferedWriter.newLine();
                bufferedWriter.close();
                fileSystem.close();
            }
            return "OK";
            
        }catch(Exception e){
            LOGGER.info("[HdfsService/readFile] erreur = "+ e.getMessage());
            return "Erreur= "+e.getMessage();
        }
    }

    /*
     * createDirectory(String directory)
     */
    public String createDirectory(String directory) {

        LOGGER.info("[HdfsService/createDirectory] createDirectory = "+ directory);
        try {
            Configuration conf =new Configuration();
            conf.set(hadoopFS, hadoopUri);
            FileSystem fileSystem=FileSystem.get(conf);
            Path path = new Path(directory);
            fileSystem.mkdirs(path);
            
        }catch(Exception e){
            LOGGER.info("[HdfsService/createDirectory] erreur = "+ e.getMessage());
            return "Erreur= "+e.getMessage();
        }
        return "OK";
    }

    /*
     * createDirectory(String directory)
     */
    public String deleteFile(String fileName) {

        LOGGER.info("[HdfsService/deleteFile] createDirectory = "+ fileName);
        try {
            Configuration conf =new Configuration();
            conf.set(hadoopFS, hadoopUri);
            FileSystem fileSystem=FileSystem.get(conf);
            Path path = new Path(fileName);
            fileSystem.delete(path, true);
            
        }catch(Exception e){
            LOGGER.info("[HdfsService/deleteFile] erreur = "+ e.getMessage());
            return "Erreur= "+e.getMessage();
        }
        return "OK";
    }
}

