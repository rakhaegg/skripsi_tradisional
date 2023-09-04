package org.example.controller;


import com.example.backservice.service.ExtractionFeatureService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class ExtractionFeatureController {
    private final ExtractionFeatureService extractionFeatureService;
    @Autowired
    public ExtractionFeatureController(ExtractionFeatureService extractionFeatureService){
        this.extractionFeatureService = extractionFeatureService;
    }

    @GetMapping("/extract")
    public void controllerExtract(
            @RequestParam("name_file") String nameFile
    ){
        this.extractionFeatureService.launchJobService(nameFile);
    }
    @GetMapping("/test")
    public void test(
    ){
        this.extractionFeatureService.test();
    }
}
