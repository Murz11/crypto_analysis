package org.example.crypto.startup;

import jakarta.annotation.PostConstruct;
import org.example.crypto.service.AutoUpdateService;
import org.example.crypto.service.CoinGeckoService;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("prod")
public class ProdStartupRunner {

    private final CoinGeckoService coinGeckoService;
    private final AutoUpdateService autoUpdateService;

    public ProdStartupRunner(
            CoinGeckoService coinGeckoService,
            AutoUpdateService autoUpdateService
    ) {
        this.coinGeckoService = coinGeckoService;
        this.autoUpdateService = autoUpdateService;
    }

    @PostConstruct
    public void start() {
        coinGeckoService.initializeHistoricalDataIfNeeded(90);
        autoUpdateService.startAutoUpdate();
    }
}
