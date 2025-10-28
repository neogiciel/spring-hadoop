package com.neogiciel.spring_hadoop;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@RestController
@RequestMapping("/hdfs")
public class HdfsController {

    /*Logger */
    static Logger LOGGER = LoggerFactory.getLogger(HdfsController.class);
 
    @Autowired
    private HdfsService hdfsService;

    
   @GetMapping("/read")
   public String readFile(@RequestParam String filename) {
      LOGGER.info("[HdfsController/readFile] ---------------------");
      LOGGER.info("[HdfsController/readFile] read = "+filename);
      return  hdfsService.readFile(filename);       
   }

   @PostMapping("/write")
    public String writeFile(@RequestParam String filename, @RequestParam String body) {
      LOGGER.info("[HdfsController/writeFile] ---------------------");
      LOGGER.info("[HdfsController/writeFile] filename = "+filename);
      LOGGER.info("[HdfsController/writeFile] body = "+body);
      return hdfsService.writeFile(filename,body);
   }

   @PostMapping("/directory")
    public String createDirectory(@RequestParam String directory) {
      LOGGER.info("[HdfsController/createDirectory] ---------------------");
      LOGGER.info("[HdfsController/createDirectory] directory = "+directory);
      return hdfsService.createDirectory(directory);
   }


   @PostMapping("/delete")
    public String deleteFile(@RequestParam String filename) {
      LOGGER.info("[HdfsController/deleteFile] ---------------------");
      LOGGER.info("[HdfsController/deleteFile] filename = "+filename);
      return hdfsService.deleteFile(filename);
   }


  
}


