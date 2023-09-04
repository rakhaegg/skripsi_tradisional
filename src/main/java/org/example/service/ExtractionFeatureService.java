package org.example.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.FileNotFoundException;

@Service
@Slf4j
public class ExtractionFeatureService{
    private final Extraction extraction;

    @Autowired
    public ExtractionFeatureService(Extraction extraction){
        this.extraction = extraction;
    }
    public void launchJobService(String nameFile){
        this.extraction.setNameFile(nameFile);
        this.extraction.setIp("192.168.88.2");
        this.extraction.setPort("14000");
        this.extraction.initData();
        this.extraction.launchJob();
        this.extraction.launchJob2();
    }
    public void test(){
        try {
            this.extraction.testCount();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
