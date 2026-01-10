package org.example.crypto.spark;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.apache.spark.sql.expressions.WindowSpec;

import java.util.Properties;
import java.sql.Timestamp;

@Component
public class SparkBatchProcessor {

    private static final Logger logger = LoggerFactory.getLogger(SparkBatchProcessor.class);

    @Value("${spring.datasource.url:jdbc:postgresql://localhost:5432/cryptodb}")
    private String dbUrl;

    @Value("${spring.datasource.username:postgres}")
    private String dbUsername;

    @Value("${spring.datasource.password:admin}")
    private String dbPassword;

    public void runDailyAnalysis() {
        logger.info("Запуск Spark анализа");

        SparkSession spark = null;
        try {
            spark = SparkSession.builder()
                    .appName("CryptoDailyAnalysis")
                    .master("local[*]")
                    .config("spark.sql.adaptive.enabled", "true")
                    .getOrCreate();

            Properties connectionProperties = new Properties();
            connectionProperties.setProperty("driver", "org.postgresql.Driver");
            connectionProperties.setProperty("user", dbUsername);
            connectionProperties.setProperty("password", dbPassword);

            Timestamp analysisTimestamp = new Timestamp(System.currentTimeMillis());

            runMarketCapWeeklyRanking(spark, dbUrl, connectionProperties, analysisTimestamp);
            runVolumeWeeklyRanking(spark, dbUrl, connectionProperties, analysisTimestamp);
            runVolumeAnalysis(spark, dbUrl, connectionProperties, analysisTimestamp);
            runMarketDominanceAnalysis(spark, dbUrl, connectionProperties, analysisTimestamp);
            runAveragePriceAnalysis(spark, dbUrl, connectionProperties, analysisTimestamp);
            runLastPriceInfo(spark, dbUrl, connectionProperties, analysisTimestamp);
            runDailyPriceChange(spark, dbUrl, connectionProperties, analysisTimestamp);
            runWeeklyVolatility(spark, dbUrl, connectionProperties, analysisTimestamp);

            logger.info("Spark анализ завершен успешно");

        } catch (Exception e) {
            logger.error("Ошибка Spark анализа: {}", e.getMessage(), e);
            throw new RuntimeException("Spark анализ не удался", e);
        } finally {
            if (spark != null) {
                spark.stop();
                logger.info("Spark сессия остановлена");
            }
        }

    }
    private void runMarketCapWeeklyRanking(SparkSession spark, String url, Properties properties, Timestamp analysisTimestamp) {
        Dataset<Row> coinData = spark.read().jdbc(url, "coin_price_history", properties);

        Dataset<Row> coinWithWeek = coinData.withColumn("week_start", functions.date_trunc("week", coinData.col("timestamp")));

        WindowSpec latestPerDay = Window
                .partitionBy("coin_id", "week_start")
                .orderBy(functions.desc("timestamp"));

        Dataset<Row> latestPerWeek = coinWithWeek
                .withColumn("rank", functions.row_number().over(latestPerDay))
                .filter(functions.col("rank").equalTo(1))
                .drop("rank");

        Dataset<Row> weeklyAggregated = latestPerWeek.groupBy("coin_id", "symbol", "week_start")
                .agg(functions.sum("market_cap").alias("weekly_market_cap"));

        WindowSpec rankingWindow = Window.partitionBy("week_start").orderBy(functions.desc("weekly_market_cap"));

        Dataset<Row> ranked = weeklyAggregated
                .withColumn("rank_position", functions.row_number().over(rankingWindow))
                .withColumn("analysis_timestamp", functions.lit(analysisTimestamp))
                .select("coin_id", "symbol", "week_start", "weekly_market_cap", "rank_position", "analysis_timestamp");

        logger.info("Weekly Market Cap Ranking:");
        ranked.show(20);

        ranked.write().mode(SaveMode.Overwrite).jdbc(url, "spark_marketcap_weekly_ranking", properties);
    }


    private void runVolumeWeeklyRanking(SparkSession spark, String url, Properties properties, Timestamp analysisTimestamp) {
        Dataset<Row> coinData = spark.read().jdbc(url, "coin_price_history", properties);

        Dataset<Row> coinWithWeek = coinData.withColumn("week_start", functions.date_trunc("week", coinData.col("timestamp")));

        WindowSpec latestPerDay = Window
                .partitionBy("coin_id", "week_start")
                .orderBy(functions.desc("timestamp"));

        Dataset<Row> latestPerWeek = coinWithWeek
                .withColumn("rank", functions.row_number().over(latestPerDay))
                .filter(functions.col("rank").equalTo(1))
                .drop("rank");

        Dataset<Row> weeklyAggregated = latestPerWeek.groupBy("coin_id", "symbol", "week_start")
                .agg(functions.sum("volume").alias("weekly_volume"));

        WindowSpec rankingWindow = Window.partitionBy("week_start").orderBy(functions.desc("weekly_volume"));

        Dataset<Row> ranked = weeklyAggregated
                .withColumn("rank_position", functions.row_number().over(rankingWindow))
                .withColumn("analysis_timestamp", functions.lit(analysisTimestamp))
                .select("coin_id", "symbol", "week_start", "weekly_volume", "rank_position", "analysis_timestamp");

        logger.info("Weekly Volume Ranking:");
        ranked.show(20);

        ranked.write().mode(SaveMode.Overwrite).jdbc(url, "spark_volume_weekly_ranking", properties);
    }



    private void runMarketDominanceAnalysis(SparkSession spark, String url, Properties properties, Timestamp analysisTimestamp) {
        Dataset<Row> coinData = spark.read().jdbc(url, "coin_price_history", properties);

        WindowSpec windowSpec = Window.partitionBy("coin_id").orderBy(functions.desc("timestamp"));
        Dataset<Row> latestCoinData = coinData
                .withColumn("rank", functions.row_number().over(windowSpec))
                .filter(functions.col("rank").equalTo(1))
                .drop("rank");

        Column totalMarketCap = functions.sum("market_cap").over();
        Dataset<Row> dominance = latestCoinData
                .withColumn("total_market_cap", totalMarketCap)
                .withColumn("market_dominance_pct",
                        functions.round(functions.col("market_cap")
                                .divide(functions.col("total_market_cap"))
                                .multiply(100), 2))
                .select("coin_id", "symbol", "market_cap", "market_dominance_pct");

        Dataset<Row> resultWithTimestamp = dominance
                .withColumn("analysis_timestamp", functions.lit(analysisTimestamp));

        logger.info("Market dominance по монетам:");
        resultWithTimestamp.show();

        resultWithTimestamp.write().mode(SaveMode.Append).jdbc(url, "spark_market_dominance_history", properties);
    }

    private void runVolumeAnalysis(SparkSession spark, String url, Properties properties, Timestamp analysisTimestamp) {
        Dataset<Row> coinData = spark.read().jdbc(url, "coin_price_history", properties);

        Dataset<Row> dailyVolume = coinData
                .withColumn("date", functions.to_date(coinData.col("timestamp")))
                .groupBy("coin_id", "symbol", "date")
                .agg(
                        functions.sum("volume").alias("daily_volume")
                );

        WindowSpec window = Window.partitionBy("coin_id", "symbol").orderBy("date");
        Dataset<Row> volumeChange = dailyVolume
                .withColumn("prev_day_volume", functions.lag("daily_volume", 1).over(window))
                .filter(functions.col("prev_day_volume").isNotNull())
                .withColumn("volume_change_pct",
                        functions.round(
                                functions.col("daily_volume")
                                        .minus(functions.col("prev_day_volume"))
                                        .divide(functions.col("prev_day_volume"))
                                        .multiply(100),
                                2
                        ));

        Dataset<Row> resultWithTimestamp = volumeChange
                .withColumn("analysis_timestamp", functions.lit(analysisTimestamp))
                .select("coin_id", "symbol", "date", "daily_volume", "volume_change_pct", "analysis_timestamp");

        logger.info("Анализ объёма торгов:");
        resultWithTimestamp.show(20);

        resultWithTimestamp.write().mode(SaveMode.Append).jdbc(url, "spark_volume_analysis_history", properties);
    }




    private void runAveragePriceAnalysis(SparkSession spark, String url, Properties properties, Timestamp analysisTimestamp) {
        Dataset<Row> coinData = spark.read().jdbc(url, "coin_price_history", properties);

        Dataset<Row> avgPrice = coinData.groupBy("symbol")
                .agg(
                        functions.avg("price").alias("avg_price"),
                        functions.min("price").alias("min_price"),
                        functions.max("price").alias("max_price"),
                        functions.count("price").alias("record_count")
                );

        Dataset<Row> resultWithTimestamp = avgPrice
                .withColumn("analysis_timestamp", functions.lit(analysisTimestamp));

        logger.info("Средняя цена монет:");
        resultWithTimestamp.show();

        resultWithTimestamp.write().mode(SaveMode.Append).jdbc(url, "spark_avg_price_history", properties);
    }

    private void runLastPriceInfo(SparkSession spark, String url, Properties properties, Timestamp analysisTimestamp) {
        Dataset<Row> coinData = spark.read().jdbc(url, "coin_price_history", properties);

        Dataset<Row> coin = coinData.as("coin");

        Dataset<Row> lastTimestamps = coin.groupBy(
                        functions.col("coin.coin_id"),
                        functions.col("coin.symbol")
                )
                .agg(functions.max(functions.col("coin.timestamp")).alias("last_collected"))
                .as("last_ts");

        Column joinCond = functions.col("coin.coin_id").equalTo(functions.col("last_ts.coin_id"))
                .and(functions.col("coin.symbol").equalTo(functions.col("last_ts.symbol")))
                .and(functions.col("coin.timestamp").equalTo(functions.col("last_ts.last_collected")));

        Dataset<Row> lastPrice = coin.join(lastTimestamps, joinCond)
                .select(
                        functions.col("coin.coin_id").alias("coin_id"),
                        functions.col("coin.symbol").alias("symbol"),
                        functions.col("coin.price").alias("price"),
                        functions.col("last_ts.last_collected").alias("last_collected")
                );

        Dataset<Row> resultWithTimestamp = lastPrice
                .withColumn("analysis_timestamp", functions.lit(analysisTimestamp));

        logger.info("Последняя цена и дата сбора:");
        resultWithTimestamp.show();

        resultWithTimestamp.write().mode(SaveMode.Append)
                .jdbc(url, "spark_last_price_history", properties);
    }



    private void runDailyPriceChange(SparkSession spark, String url, Properties properties, Timestamp analysisTimestamp) {
        Dataset<Row> coinData = spark.read().jdbc(url, "coin_price_history", properties);

        Dataset<Row> dailyAvg = coinData
                .withColumn("date", functions.to_date(coinData.col("timestamp")))
                .groupBy("coin_id", "symbol", "date")
                .agg(functions.avg("price").alias("daily_avg_price"));

        WindowSpec window = Window.partitionBy("coin_id", "symbol").orderBy("date");

        Dataset<Row> dailyChange = dailyAvg
                .withColumn("prev_day_avg", functions.lag("daily_avg_price", 1).over(window))
                .filter(functions.col("prev_day_avg").isNotNull()) // Исключаем первый день
                .withColumn("price_change_pct",
                        functions.round(
                                functions.col("daily_avg_price")
                                        .minus(functions.col("prev_day_avg"))
                                        .divide(functions.col("prev_day_avg"))
                                        .multiply(100),
                                2)
                )
                .withColumn("start_price", functions.col("prev_day_avg"))
                .withColumn("end_price", functions.col("daily_avg_price"));

        Dataset<Row> resultWithTimestamp = dailyChange
                .withColumn("analysis_timestamp", functions.lit(analysisTimestamp))
                .select("coin_id", "symbol", "date", "start_price", "end_price", "price_change_pct", "analysis_timestamp");

        logger.info("Изменение цены между днями:");
        resultWithTimestamp.show(20);

        resultWithTimestamp.write().mode(SaveMode.Append).jdbc(url, "spark_daily_change_history", properties);
    }

    private void runWeeklyVolatility(SparkSession spark, String url, Properties properties, Timestamp analysisTimestamp) {
        Dataset<Row> coinData = spark.read().jdbc(url, "coin_price_history", properties);

        Dataset<Row> weeklyData = coinData
                .withColumn("week", functions.date_trunc("week", coinData.col("timestamp")))
                .groupBy("coin_id", "symbol", "week")
                .agg(
                        functions.count("price").alias("record_count"),
                        functions.max("price").alias("max_price"),
                        functions.min("price").alias("min_price"),
                        functions.avg("price").alias("avg_price"),
                        functions.stddev("price").alias("price_stddev")
                )
                .filter(functions.col("record_count").geq(3)); // Только недели с 3+ записями

        Dataset<Row> volatility = weeklyData
                .withColumn("price_range",
                        functions.col("max_price").minus(functions.col("min_price")))
                .withColumn("volatility_pct",
                        functions.round(
                                functions.col("price_stddev")
                                        .divide(functions.col("avg_price"))
                                        .multiply(100),
                                2)
                );

        Dataset<Row> resultWithTimestamp = volatility
                .withColumn("analysis_timestamp", functions.lit(analysisTimestamp))
                .select("coin_id", "symbol", "week", "price_range", "volatility_pct", "record_count", "analysis_timestamp");

        logger.info("Недельная волатильность:");
        resultWithTimestamp.show(20);

        resultWithTimestamp.write().mode(SaveMode.Append).jdbc(url, "spark_weekly_volatility_history", properties);
    }
}