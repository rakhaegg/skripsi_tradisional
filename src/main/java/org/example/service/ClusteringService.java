package org.example.service;


import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class ClusteringService {
    private final Clustering clustering;
    @Autowired
    public ClusteringService(Clustering clustering){
        this.clustering = clustering;
    }
    public void fitClusteringService(
            String nameFile,
            String uuid,
            String iteration
    ){
        this.clustering.setIp("192.168.88.2");
        this.clustering.setPort("14000");
        this.clustering.setName(nameFile);
        this.clustering.setUuid(uuid);
        this.clustering.setIteration(Integer.parseInt(iteration));
        this.clustering.setInitCentroid();
        this.clustering.getDataInputFromHDFS();
        this.clustering.fit();
    }
}
