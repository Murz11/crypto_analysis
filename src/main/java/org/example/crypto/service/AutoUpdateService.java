package org.example.crypto.service;

import lombok.Getter;
import org.example.crypto.spark.SparkBatchProcessor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
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

    @Autowired
    public AutoUpdateService(CoinGeckoService coinGeckoService, SparkBatchProcessor sparkBatchProcessor) {
        this.coinGeckoService = coinGeckoService;
        this.sparkBatchProcessor = sparkBatchProcessor;
    }

    @PostConstruct
    public void init() {
        this.scheduler = Executors.newScheduledThreadPool(2);
        logger.info("Сервис автообновления инициализирован");
    }

    @PreDestroy
    public void cleanup() {
        stopAutoUpdate();
    }

    public void startAutoUpdate() {
        if (!autoUpdateEnabled) {
            autoUpdateEnabled = true;

            scheduler.scheduleAtFixedRate(this::updateData, 0, 1, TimeUnit.MINUTES);
            scheduler.scheduleAtFixedRate(this::runSparkAnalysis, 0, 2, TimeUnit.MINUTES);

            logger.info("Автообновление запущено:");
            logger.info("Данные: каждую минуту");
            logger.info("Spark анализ: каждые 2 минуты");
        } else {
            logger.warn("Автообновление уже запущено");
        }
    }

    public void stopAutoUpdate() {
        if (autoUpdateEnabled) {
            autoUpdateEnabled = false;
            if (scheduler != null) {
                scheduler.shutdown();
                try {
                    if (!scheduler.awaitTermination(30, TimeUnit.SECONDS)) {
                        scheduler.shutdownNow();
                        logger.warn("Планировщик был принудительно остановлен");
                    }
                } catch (InterruptedException e) {
                    scheduler.shutdownNow();
                    logger.warn("Планировщик остановлен из-за прерывания");
                    Thread.currentThread().interrupt();
                }
            }
            logger.info("Автообновление остановлено");
        } else {
            logger.warn("Автообновление уже остановлено");
        }
    }

    private void updateData() {
        try {
            logger.debug("Запуск автообновления данных");
            coinGeckoService.fetchAndSaveCoinData();
            logger.info("Данные успешно обновлены в {}",
                    LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")));
        } catch (Exception e) {
            logger.error("Ошибка при автообновлении данных: {}", e.getMessage(), e);
        }
    }

    private void runSparkAnalysis() {
        try {
            logger.debug("Запуск Spark анализа");
            sparkBatchProcessor.runDailyAnalysis();
            logger.info("Spark анализ успешно завершен в {}",
                    LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")));
        } catch (Exception e) {
            logger.error("Ошибка при выполнении Spark анализа: {}", e.getMessage(), e);
        }
    }

    public String getStatus() {
        String status = autoUpdateEnabled ? "Активно" : "Остановлено";
        logger.debug("Запрос статуса сервиса: {}", status);
        return status;
    }
}