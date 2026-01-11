package org.example.crypto.service;

import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.crypto.spark.SparkBatchProcessor;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Service
public class AutoUpdateService {

    private static final Logger logger = LogManager.getLogger(AutoUpdateService.class);

    private final CoinGeckoService coinGeckoService;
    private final SparkBatchProcessor sparkBatchProcessor;

    private ScheduledExecutorService scheduler;

    @Getter
    private boolean autoUpdateEnabled = false;

    public AutoUpdateService(
            CoinGeckoService coinGeckoService,
            SparkBatchProcessor sparkBatchProcessor
    ) {
        this.coinGeckoService = coinGeckoService;
        this.sparkBatchProcessor = sparkBatchProcessor;
    }

    @PostConstruct
    public void init() {
        scheduler = Executors.newScheduledThreadPool(1);
        logger.info("Сервис автообновления инициализирован");
    }

    @PreDestroy
    public void cleanup() {
        stopAutoUpdate();
    }

    public void startAutoUpdate() {
        if (autoUpdateEnabled) {
            logger.warn("Автообновление уже запущено");
            return;
        }

        autoUpdateEnabled = true;

        scheduler.scheduleAtFixedRate(
                this::updateDataAndSpark,
                0,
                1,
                TimeUnit.MINUTES
        );

        logger.info("Автообновление с Spark анализом запущено");
    }

    public void stopAutoUpdate() {
        if (!autoUpdateEnabled) {
            logger.warn("Автообновление уже остановлено");
            return;
        }

        autoUpdateEnabled = false;

        if (scheduler != null) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(30, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        logger.info("Автообновление остановлено");
    }

    private void updateDataAndSpark() {
        try {
            logger.debug("Запуск автообновления данных");
            coinGeckoService.fetchAndSaveCoinData();
            logger.info(
                    "Данные успешно обновлены в {}",
                    LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"))
            );

            logger.debug("Запуск Spark анализа после обновления");
            sparkBatchProcessor.runDailyAnalysis();
            logger.info(
                    "Spark анализ завершён в {}",
                    LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"))
            );

        } catch (Exception e) {
            logger.error("Ошибка при автообновлении или Spark анализе", e);
        }
    }

    public String getStatus() {
        return autoUpdateEnabled ? "Активно" : "Остановлено";
    }
}
