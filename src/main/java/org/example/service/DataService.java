package org.example.service;


import lombok.extern.slf4j.Slf4j;
import org.apache.hc.client5.http.classic.methods.HttpPut;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.FileEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

@Service
@Slf4j
public class DataService {
    private final String url = "http://192.168.88.2:14000/webhdfs/v1/";

    public File writeFileTemporaryToLocalDist(MultipartFile file) throws IOException {
        String pathWriteTemporary = System.getProperty("java.io.tmpdir")+"\\tmpfile";
        File fileTemp = new File(pathWriteTemporary);
        if(fileTemp.exists()){
            fileTemp.delete();
        }
        file.transferTo(fileTemp);
        return fileTemp;
    }
    public void initProcessBuilder(String name){
        String nameFile = name.replaceAll(".json","");
        List<String> commandProcess = List.of("bash", "run_job.sh",
                "1",
                "0",
                "0",
                "0",
                "0",
                "0",
                nameFile,
                "0");
        ProcessBuilder processBuilder = new ProcessBuilder(commandProcess);
        File file = new File("F:\\scala\\final_project\\mapreduceService\\");
        processBuilder.directory(file);
        try {
            Process process = processBuilder.start();
            try(BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream()))){
                String line;
                while((line =  bufferedReader.readLine()) != null){
                    log.info(line);
                }
            }
            int exitCode = process.waitFor();
            log.info(String.valueOf(exitCode));
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    public void serviceUploadData(MultipartFile file){
        try(FileEntity fileEntity = new FileEntity(writeFileTemporaryToLocalDist(file), ContentType.APPLICATION_OCTET_STREAM);
            CloseableHttpClient closeableHttpClient = HttpClientBuilder.create().build()) {
                String urlUploadData = url + "/data/"+file.getOriginalFilename()+
                        "?op=CREATE&"+
                        "overwrite=true&"+
                        "user.name=hadoopuser";
                HttpPut httpPut = new HttpPut(urlUploadData);
                httpPut.setEntity(fileEntity);
                closeableHttpClient.execute(httpPut,response->{
                    HttpEntity http = response.getEntity();
                    String result = EntityUtils.toString(http);
                    log.info(result);
                    EntityUtils.consume(http);
                    return response;
                });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
