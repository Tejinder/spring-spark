package com.tejinder.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import com.tejinder.bean.SparkCount;
import com.tejinder.service.SparkWordCount;

@RequestMapping("api")
@Controller
public class SparkController {
    @Autowired
    SparkWordCount sparkWordCount;

    @RequestMapping("wordcount")
    public ResponseEntity<List<SparkCount>> words() {
        return new ResponseEntity<>(sparkWordCount.count(), HttpStatus.OK);
    }
}