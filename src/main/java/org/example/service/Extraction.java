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
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Service
@RequestScope
@Getter
@Setter
@Slf4j
public class Extraction {
    private String nameFile;
    private String ip;
    private String port;
    private Long lengthDocument;
    private int maxDF = 1;
    private int minDF = 0;
    public Extraction(){

    }
    public void testCount() throws FileNotFoundException {
        String outputDir = "D:\\output_2\\part-r-00000";
        try(BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(outputDir)))){
            String line;
            int count = 0;
            while((line = bufferedReader.readLine()) != null){
                count++;
            }
            log.info(String.valueOf(count));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public void initData(){
        String url = "http://"+ip+":"+port+"/webhdfs/v1/output/"+nameFile+
                "/jumlah_dokumen.txt?"+
                "op=OPEN&"+
                "user.name=hadoopuser";
        AtomicReference<String> result = new AtomicReference<>();
        try(CloseableHttpClient closeableHttpClient = HttpClientBuilder.create().build()){
            HttpGet httpGet = new HttpGet(url);
            closeableHttpClient.execute(httpGet,response->{
                HttpEntity http = response.getEntity();
                this.lengthDocument = Long.valueOf(EntityUtils.toString(http));
                EntityUtils.consume(http);
                return response;
            });
            String url2 = "http://"+ip+":"+port+"/webhdfs/v1/output/"+nameFile+
                    "/part-r-00000?"+
                    "op=OPEN&"+
                    "user.name=hadoopuser";
            HttpGet httpGet1 = new HttpGet(url2);
            closeableHttpClient.execute(httpGet1,response->{
                HttpEntity http = response.getEntity();
                result.set(EntityUtils.toString(http));
                EntityUtils.consume(http);
                return response;

            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        String tmpDir = "D:\\output\\part-r-00000";
        File file = new File(tmpDir);
        if(file.exists()){
            file.delete();
        }
        try(FileWriter fileWriter = new FileWriter(file)){
            fileWriter.write(result.get());
        }catch (IOException e){
            throw new RuntimeException(e);
        }

    }
    private Map<String, List<String>> convertToMap(String[] value, String documentID){
        return Arrays.stream(value)
                .collect(Collectors.groupingBy(
                        item -> item,
                        Collectors.mapping(item -> "1:"+documentID, Collectors.toList() )));

    }
    public void launchJob(){
        String outputTmpDir = "D:\\output_1\\part-r-00000";
        String inputDir = "D:\\output\\part-r-00000";
        Map<String,List<String>> tmp1 = new HashMap<>();

        try(BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(inputDir)))){
            String line;
            while((line = bufferedReader.readLine()) != null){
                String[] inputData = line.split("\t");
                String documentID = inputData[0];
                String[] arrayOfWord = inputData[1].substring(1 , inputData[1].length()-1).split(", ");
                convertToMap(arrayOfWord,documentID)
                        .forEach((map_key,map_value) -> {
                            map_value.forEach(item_map_value->{
                                    if(!tmp1.containsKey(map_key)){
                                        List<String>newList = new ArrayList<>();
                                        newList.add(item_map_value);
                                        tmp1.put(map_key,newList);
                                    }else{
                                        List<String> old = tmp1.get(map_key);
                                        old.add(item_map_value);
                                        tmp1.put(map_key,old);
                                    }
                            });
                        });
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        File file = new File(outputTmpDir);
        if(file.exists()){
            file.delete();
        }
        tmp1.forEach((key,value)->{
            String word = key;
            AtomicInteger count = new AtomicInteger();

            List<String> listDocID = value.stream().
                    peek(item -> count.addAndGet(Integer.parseInt(item.toString().split(":")[0])))
                    .map(item -> item.split(":")[1])
                    .collect(Collectors.toList());
            Set<String> uniqueID = new HashSet<>(listDocID);

            int finalMaxDf =  Math.round(maxDF* lengthDocument);

            int finalMinDF=  Math.round(minDF * lengthDocument);
            if(uniqueID.size() >= finalMinDF && uniqueID.size() <= finalMaxDf){
                try(BufferedWriter fileWriter = new BufferedWriter(new FileWriter(file,true))){
                    fileWriter.write(word + "\t" + uniqueID.size() + "\t" + listDocID.size() + "\t" + listDocID.toString());
                    fileWriter.newLine();
                }catch (IOException e){
                    throw new RuntimeException(e);
                }
            }
        });
    }
    public void launchJob2(){
        String inputDir = "D:\\output\\part-r-00000";
        String inputDirFeature = "D:\\output_1\\part-r-00000";
        String outputDir = "D:\\output_2\\part-r-00000";
        File file = new File(outputDir);
        if(file.exists()){
            file.delete();
        }
        Map<String,Integer> wordFeature = new HashMap<>();
        try(BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(inputDirFeature)))){
            String line;
            while((line = bufferedReader.readLine()) != null){
                wordFeature.put(line.split("\t")[0],Integer.parseInt(line.split("\t")[2]));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try(BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(inputDir)))){
          String line;
          while((line = bufferedReader.readLine()) != null){
              String[] inputData = line.split("\t");
              String docID = inputData[0];
              List<String> dataKata =
                      Arrays.asList(inputData[1].substring(1,inputData[1].length()-1).split(", "));
              TreeMap<String,Integer> result = new TreeMap<>();
              AtomicInteger count = new AtomicInteger();
              dataKata.forEach(item->{
                  if(wordFeature.containsKey(item)){
                      if(!result.containsKey(item)){
                          result.put(item,1);
                      }else{
                          result.put(item, result.get(item)+1);
                      }
                  }
                  count.getAndIncrement();
              });
              wordFeature.forEach((word,valueDF)->{
                  if(!result.containsKey(word)){
                      result.put(word,0);
                  }
              });
              List<String> data =new ArrayList<>();
              result.forEach((key,value)->{
                  data.add("("+key+","+wordFeature.get(key)+","+value+")");
              });
              try(BufferedWriter fileWriter = new BufferedWriter(new FileWriter(file,true))){
                  fileWriter.write(docID + "\t" + data.toString() + "\t" + count);
                  fileWriter.newLine();
              }catch (IOException e){
                  throw new RuntimeException(e);
              }
          }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
