package org.example.service;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.springframework.stereotype.Service;
import org.springframework.web.context.annotation.RequestScope;

import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@Service
@RequestScope
@Getter
@Setter
@Slf4j
public class Clustering {
    private Map<Integer, List<Integer>> oldGroup = new HashMap<>();
    private Map<Integer,List<Integer>> newGroup = new HashMap<>();
    private Map<Integer,Map<String,Double>> centroid = new HashMap<>();
    private Map<Integer,Map<String,Double>> oldCentroid = new HashMap<>();

    private String ip;
    private String port;
    private String name;
    private String uuid;
    private int iteration;
    public Clustering(){

    }
    public void setInitCentroid(){
        log.info("START SETTING CENTROID");
        String centroidDir = "F:\\tmp_input\\"+this.name+"\\centroid";
        File fileInputCentroid = new File(centroidDir);
        if(!fileInputCentroid.getParentFile().exists()){
            fileInputCentroid.getParentFile().mkdir();
        }
        if(!fileInputCentroid.exists()){
            try(CloseableHttpClient httpClient = HttpClientBuilder.create().build()){
                String outputPath = "/output_2/"+this.name+"/"+this.uuid+"/centroid";
                String urlPathCentroid = "http://"+ip+":"+port+"/webhdfs/v1"+outputPath+
                        "?op=OPEN&"+
                        "user.name=hadoopuser";
                HttpGet httpGet = new HttpGet(urlPathCentroid);
                httpClient.execute(httpGet,response->{
                    HttpEntity http = response.getEntity();
                    String result = EntityUtils.toString(http);
                    try(BufferedWriter bufferedWriter =new BufferedWriter(new FileWriter(fileInputCentroid,true))){
                        for(String splitResult : result.split("\n")){
                            if(!splitResult.isEmpty()){
                                bufferedWriter.append(splitResult);
                                bufferedWriter.newLine();
                            };

                        }
                    }
                    EntityUtils.consume(http);
                    return response;
                });
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        try(BufferedReader bufferedReader1 = new BufferedReader(new InputStreamReader(new FileInputStream(fileInputCentroid)))){
            String lineCentroid;
            while((lineCentroid = bufferedReader1.readLine()) != null){
                String[] splitLineCentroid =  lineCentroid.split("\t");
                String indexCluster  = splitLineCentroid[0];
                Map<String,Double> temp = new HashMap<>();
                Arrays.stream(splitLineCentroid[1].substring(1,splitLineCentroid[1].length()-1)
                                .split(", "))
                        .forEach(itemWord->{
                            String[] splitItemWord = itemWord.substring(1 , itemWord.length()-1).split(":");
                            String word = splitItemWord[0];
                            Double wordValueCentroid  = Double.parseDouble(splitItemWord[1]);
                            temp.put(word,wordValueCentroid);
                        });
                centroid.put(Integer.parseInt(indexCluster),temp);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        log.info("END SETTING CENTROID");
    }
    public void getDataInputFromHDFS(){
        log.info("START SETTING INPUT DATA");
        List<String> listUrlInputData = new ArrayList<>();

        String fileInputPathString = "F:\\tmp_input\\"+this.name+"\\input";
        File fileInputPath = new File(fileInputPathString);
        log.info(fileInputPath.getParent());
        if(!fileInputPath.getParentFile().exists()){
            fileInputPath.getParentFile().mkdir();
        }
        if(!fileInputPath.exists()) {
            for (int i = 0; i < 5; i++) {
                String path = "/output_2/"+this.name+"/"+this.uuid+"/text-r-0000" + i;
                String urlPath = "http://" + this.ip + ":" +this.port +"/webhdfs/v1" + path +
                        "?op=OPEN&" +
                        "user.name=hadoopuser";
                listUrlInputData.add(urlPath);
            }
            try (CloseableHttpClient closeableHttpClient = HttpClientBuilder.create().build()) {
                for (String urlPath : listUrlInputData) {
                    HttpGet httpGet = new HttpGet(urlPath);
                    closeableHttpClient.execute(httpGet, response -> {
                        HttpEntity httpEntity = response.getEntity();
                        String result = EntityUtils.toString(httpEntity);
                        try(BufferedWriter writer = new BufferedWriter(new FileWriter(fileInputPath,true))){
                            writer.append(result);
                            writer.newLine();;
                        }
                        EntityUtils.consume(httpEntity);
                        return response;
                    });
                }

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        log.info("END SETTING INPUT DATA");

    }
    public void fit(){
        log.info("START CLUSTERING");
        String inputDir = "F:\\tmp_input\\"+this.name+"\\input";
        String centroidDir = "F:\\tmp_input\\"+this.name+"\\centroid";
        File fileInput = new File(inputDir);
        File fileInputCentroid = new File(centroidDir);

        for(int i = 0 ; i < this.iteration; i++) {
            long startTime = System.currentTimeMillis();
            Map<Integer, List<String>> temp = new HashMap<>();
            try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(fileInput)))) {
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    if(!line.isEmpty()) {
                        String[] splitLine = line.split("\t");
                        String documentID = splitLine[0];
                        Map<Integer, Double> distanceBasedCluster = new HashMap<>();

                        Map<String, Double> mapInputValue = new HashMap<>();
                        Arrays.stream(splitLine[1].substring(1, splitLine[1].length() - 1).split(", "))
                                .forEach(item -> {
                                    String[] itemSplit = item.substring(1, item.length() - 1).split(":");
                                    mapInputValue.put(itemSplit[0], Double.parseDouble(itemSplit[1]));
                                });
                        centroid.forEach((keyCentroid, valueCentroid) -> {
                            AtomicReference<Double> sum = new AtomicReference<>((double) 0);
                            valueCentroid.forEach((word, wordValue) -> {
                                Double inputTFIDF = mapInputValue.get(word);
                                Double result = Math.pow((inputTFIDF - wordValue), 2);
                                sum.set(sum.get() + result);
                            });
                            distanceBasedCluster.put(keyCentroid, Math.sqrt(sum.get()));
                        });

                        Map.Entry<Integer, Double> minEntry = distanceBasedCluster
                                .entrySet().stream()
                                .min(Map.Entry.comparingByValue())
                                .orElse(null);
                        assert minEntry != null;
                        if (!temp.containsKey(minEntry.getKey())) {
                            List<String> initList = new ArrayList<>();
                            initList.add(line + "\t" + minEntry.getValue());
                            temp.put(minEntry.getKey(), initList);
                        } else {
                            List<String> oldList = temp.get(minEntry.getKey());
                            oldList.add(line + "\t" + minEntry.getValue());
                            temp.put(minEntry.getKey(), oldList);
                        }
                    }
                }
                Map<Integer, String> tmpNewCentroid = new HashMap<>();
                Map<Integer, Double> sseMap = new HashMap<>();
                temp.forEach((indexCluster, groupCluster) -> {
                    Map<String, Double> mapAverage = new HashMap<>();
                    AtomicInteger count = new AtomicInteger();
                    AtomicReference<Double> sse = new AtomicReference<>(0.0);
                    groupCluster.forEach(item -> {
                        String[] splitValue = item.split("\t");
                        String documentID = splitValue[0];
                        BigDecimal decimal = BigDecimal.valueOf(Double.parseDouble(splitValue[2]));
                        BigDecimal round = decimal.setScale(10, RoundingMode.HALF_UP);
                        sse.set(sse.get() + Math.pow(round.doubleValue(), 2));
                        Arrays.stream(splitValue[1].substring(1, splitValue[1].length() - 1).split(", "))
                                .forEach(item2 -> {
                                    String[] splitItem2 = item2.substring(1, item2.length() - 1).split(":");
                                    String word = splitItem2[0];
                                    Double valueWord = Double.parseDouble(splitItem2[1]);
                                    if (!mapAverage.containsKey(word)) {
                                        mapAverage.put(word, valueWord);
                                    } else {
                                        Double oldValue = mapAverage.get(word);
                                        mapAverage.put(word, (oldValue + valueWord));
                                    }
                                });
                        count.set(count.get() + 1);
                    });
                    sseMap.put(indexCluster,sse.get());
                    List<String> dataResult = new ArrayList<>();
                    mapAverage.forEach((keyValue, value) -> {
                        BigDecimal decimal = new BigDecimal(value/count.get());
                        BigDecimal round = decimal.setScale(10, RoundingMode.HALF_UP);
                        dataResult.add("(" + keyValue + ":" + round.doubleValue() + ")");
                    });
                    tmpNewCentroid.put(indexCluster, dataResult.toString());
                });

                tmpNewCentroid.forEach((keyNewCentroid, valueNewCentroid) -> {
                    Map<String, Double> tmp = new HashMap<>();
                    Arrays.stream(valueNewCentroid.substring(1, valueNewCentroid.length() - 1).split(", ")).
                            forEach(itemWordValue -> {
                                String[] splitItemWordValue = itemWordValue.substring(1, itemWordValue.length() - 1).split(":");
                                String word = splitItemWordValue[0];
                                tmp.put(word, Double.parseDouble(splitItemWordValue[1]));
                            });
                    centroid.put(keyNewCentroid, tmp);
                });
                log.info(sseMap.toString());
                if(i == 0){
                    oldCentroid = new HashMap<>(centroid);
                }else{
                    if(converge()){
                        long endTime = System.currentTimeMillis();
                        log.info("Iteasi : " + i);
                        log.info("TIME : " + (endTime-startTime)/ 1000.0);
                        log.info("CONVERGE");
                       break;
                    }
                    this.oldCentroid = new HashMap<>(centroid);
                }
                long endTime = System.currentTimeMillis();
                log.info("Iteasi : " + i);
                log.info("TIME : " + (endTime-startTime)/ 1000.0);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
    private boolean converge(){
        boolean isEqual =
                this.oldCentroid.entrySet().stream()
                        .allMatch(entry -> entry.getValue().equals(this.centroid.get(entry.getKey())));
        return isEqual;
    }


}
