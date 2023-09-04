package org.example.controller;


import com.example.backservice.service.ClusteringService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class ClusteringController {
    private final ClusteringService clusteringService;

    @Autowired
    public ClusteringController(ClusteringService clusteringService) {
        this.clusteringService = clusteringService;
    }

    @PostMapping("/cluster")
    public void fitCluster(
            @RequestParam("name") String nameFile,
            @RequestParam("uuid") String uuid,
            @RequestParam("iteration") String iteration
    ){
        this.clusteringService.fitClusteringService(
                nameFile,
                uuid,
                iteration
        );

    }
}
