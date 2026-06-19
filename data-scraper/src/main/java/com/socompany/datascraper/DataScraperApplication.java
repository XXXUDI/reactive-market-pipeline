package com.socompany.datascraper;

import com.socompany.datascraper.scraper.AggTradeScraper;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class DataScraperApplication {

    static void main(String[] args) {
        var context = SpringApplication.run(DataScraperApplication.class, args);

        AggTradeScraper scraper = context.getBean(AggTradeScraper.class);

        scraper.scrape();

    }

}