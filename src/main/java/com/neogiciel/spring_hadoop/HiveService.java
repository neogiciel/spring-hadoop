package com.neogiciel.spring_hadoop;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class HiveService {

    /*Logger */
    static Logger LOGGER = LoggerFactory.getLogger(HiveService.class);

     private static String driverClass = "org.apache.hive.jdbc.HiveDriver";
     //private static String driverClass = "org.postgresql.Driver";

    //@Autowired
    //private JdbcTemplate jdbcTemplate;

    /*
     * sql()
     */
    public String sql() {
        LOGGER.info("[HiveService/sql] sql ");
        //jdbcTemplate.queryForList("show databases");
        try {
             Class.forName(driverClass);
             Connection connection = DriverManager.getConnection("jdbc:hive2://192.168.1.12:31712/cliservice", "", "");
             Statement statement = connection.createStatement();

             String sql = "SELECT * FROM test";
             ResultSet result = statement.executeQuery(sql);
             System.out.println("Running: " + sql);
             result = statement.executeQuery(sql);
             while (result.next()) {
                    System.out.println("Id=" + result.getString(1));
                    System.out.println("Name=" + result.getString(2));
                    System.out.println("Address=" + result.getString(3));
              }
              result.close();
              statement.close();
              connection.close();



             return "OK";

        } catch (Exception exception) {
            exception.printStackTrace();
            LOGGER.info("[HiveService/sql] sql "+exception.getMessage());
            return "ERREUR";
        }
        
        
        /*
        String table = "CUSTOMER";
        try {
            statement.executeQuery("DROP TABLE " + table);
        } catch (Exception exception) {
            exception.printStackTrace();
        }
 
        try {
            statement.executeQuery("CREATE TABLE " + table + " (ID INT, NAME STRING, ADDR STRING)");
        } catch (Exception exception) {
            exception.printStackTrace();
        }
 
        String sql = "SHOW TABLES '" + table + "'";
        System.out.println("Executing Show table: " + sql);
        ResultSet result = statement.executeQuery(sql);
        if (result.next()) {
            System.out.println("Table created is :" + result.getString(1));
        }
 
        sql = "INSERT INTO CUSTOMER (ID,NAME,ADDR) VALUES (1, 'Ramesh', '3 NorthDrive SFO' )";
        System.out.println("Inserting table into customer: " + sql);
 
        try {
            statement.executeUpdate(sql);
        } catch (Exception exception) {
            exception.printStackTrace();
        }
 
        sql = "SELECT * FROM " + table;
        result = statement.executeQuery(sql);
        System.out.println("Running: " + sql);
        result = statement.executeQuery(sql);
        while (result.next()) {
            System.out.println("Id=" + result.getString(1));
            System.out.println("Name=" + result.getString(2));
            System.out.println("Address=" + result.getString(3));
        }
        result.close();
        statement.close();
        connection.close();*/
    }
    
    
}

