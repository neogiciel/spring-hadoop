package com.neogiciel.spring_hadoop;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@RestController
@RequestMapping("/spark")
public class SparkController {

    /*Logger */
    static Logger LOGGER = LoggerFactory.getLogger(SparkController.class);

    @Autowired
    private SparkService sparkService;

    
   @GetMapping("/count")
   public String readFile(@RequestParam String filename) {
      LOGGER.info("[HdfsController/readFile] ---------------------");
      LOGGER.info("[HdfsController/readFile] read = "+filename);
      //return  sparkService.count(filename);       
      return  sparkService.test(filename);       
   }

  
}


