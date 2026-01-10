package org.example.crypto.spark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


@Component
public class SparkRunner {

    private static final Logger logger = LoggerFactory.getLogger(SparkRunner.class);

    @Autowired
    private SparkBatchProcessor sparkBatchProcessor;

    public void runSparkAnalysis() {
        logger.info("Запуск встроенного Spark анализа");

        try {
            sparkBatchProcessor.runDailyAnalysis();

            logger.info("Spark анализ завершен успешно");
        } catch (Exception e) {
            logger.error("Ошибка Spark анализа: {}", e.getMessage(), e);
        }
    }
}