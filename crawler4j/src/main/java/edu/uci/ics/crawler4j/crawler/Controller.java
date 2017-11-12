package edu.uci.ics.crawler4j.crawler;

import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;

public class Controller {
    public static void main(String[] args) throws Exception {
        

            
                    String rootFolder = "data/crowler";
                    int numberOfCrawlers = 1;

                    CrawlConfig config = new CrawlConfig();
                    config.setCrawlStorageFolder(rootFolder);
                    config.setMaxPagesToFetch(50);
                    config.setPolitenessDelay(1000);
                    config.setMaxDepthOfCrawling(10);
                    /*config.setProxyHost("cache.mrt.ac.lk");
                    config.setProxyPort(3128)*/;

                    PageFetcher pageFetcher = new PageFetcher(config);
                    RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
                    RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig, pageFetcher);
                    CrawlController controller = new CrawlController(config, pageFetcher, robotstxtServer);

                    controller.addSeed("https://www.programmableweb.com/");
                    controller.start(Crawler.class, numberOfCrawlers);

            

    
    }
}
