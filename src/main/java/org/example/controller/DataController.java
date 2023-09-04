package org.example.controller;

import com.example.backservice.service.DataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.util.Objects;

@RestController
@RequestMapping("/api")
public class DataController {
    private final DataService dataService;

    @Autowired
    public DataController(DataService dataService){
        this.dataService= dataService;
    }

    @PostMapping("/upload")
    public void controllerUploadData(
            @RequestParam("file")MultipartFile file
            ){
        this.dataService.serviceUploadData(file);
        this.dataService.initProcessBuilder(Objects.requireNonNull(file.getOriginalFilename()));
    }
}
