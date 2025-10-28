package com.neogiciel.spring_hadoop;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@RestController
@RequestMapping("/hive")
public class HiveController {

    /*Logger */
    static Logger LOGGER = LoggerFactory.getLogger(SparkController.class);

    @Autowired
    private HiveService hiveService;

    
   @GetMapping("/sql")
   public String liste() {
      LOGGER.info("[HiveController/liste] ---------------------");
      return  hiveService.sql();       
      //return "toto";
   }

  
}


