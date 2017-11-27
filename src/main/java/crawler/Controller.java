package crawler;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
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
                   

                    PageFetcher pageFetcher = new PageFetcher(config);
                    RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
                    RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig, pageFetcher);
                    CrawlController controller = new CrawlController(config, pageFetcher, robotstxtServer);

                    controller.addSeed("https://www.programmableweb.com/category/all/apis");
                    controller.addSeed("https://rapidapi.com/");
                    controller.addSeed("http://www.apiforthat.com/apis");
                    controller.addSeed("https://sdks.io/");
                    controller.addSeed("https://www.producthunt.com/#!/s/posts/api");
                    controller.addSeed("https://apilist.fun/");
                    
                    
                    controller.start(Crawler.class, numberOfCrawlers);

            

    
    }
}
