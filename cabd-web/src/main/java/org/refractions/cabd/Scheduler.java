package org.refractions.cabd;

import org.refractions.cabd.model.VectorTileCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@EnableScheduling
public class Scheduler {

    private Logger logger = LoggerFactory.getLogger(Scheduler.class);

	@Autowired
	VectorTileCache cache;
	
	@Scheduled(fixedDelayString = "${cabd.cachecleanupdelay}000")
	public void cleanupCache() {
		logger.debug("cleaning barrier vector tile cache");
		cache.cleanUpCache();
	}
}
