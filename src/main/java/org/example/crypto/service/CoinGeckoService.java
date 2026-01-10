package org.example.crypto.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import org.example.crypto.model.CoinPriceHistory;
import org.example.crypto.repository.CoinPriceHistoryRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class CoinGeckoService {

    private static final Logger logger = LoggerFactory.getLogger(CoinGeckoService.class);

    private WebClient webClient;
    private final ObjectMapper objectMapper;
    private final CoinPriceHistoryRepository priceHistoryRepository;

    @Value("${crypto.coingecko.base-url:https://api.coingecko.com/api/v3}")
    private String baseUrl;

    @Value("${crypto.coingecko.request-timeout:30000}")
    private int requestTimeout;

    @Value("${crypto.coingecko.coins-to-track:bitcoin,ethereum,ethereum-classic,ripple,cardano,solana,dogecoin,polkadot,shiba-inu,polygon,litecoin,tron,stellar,vechain,monero,eos,theta,axie-infinity,crypto-com-chain,uniswap}")
    private List<String> defaultCoins;

    private final Path coinsFile = Paths.get("coins_to_track.json");

    @Getter
    private List<String> coinsToTrack;

    @Autowired
    public CoinGeckoService(CoinPriceHistoryRepository priceHistoryRepository) {
        this.objectMapper = new ObjectMapper();
        this.priceHistoryRepository = priceHistoryRepository;
    }

    @PostConstruct
    public void init() {
        this.webClient = WebClient.builder()
                .baseUrl(baseUrl)
                .build();

        coinsToTrack = loadCoinsFromFile();
        logger.info("CoinGeckoService инициализирован. Base URL: {}, Отслеживаемые монеты: {}", baseUrl, coinsToTrack);

        initializeHistoricalDataIfNeeded(90);
    }

    private List<String> loadCoinsFromFile() {
        if (Files.exists(coinsFile)) {
            try {
                String json = Files.readString(coinsFile);
                List<String> coins = objectMapper.readValue(json, new TypeReference<List<String>>() {});
                if (!coins.isEmpty()) {
                    return coins;
                }
            } catch (IOException e) {
                logger.warn("Не удалось прочитать coins_to_track.json, используем дефолтные монеты", e);
            }
        }
        return new ArrayList<>(defaultCoins);
    }

    private void saveCoinsToFile() {
        try {
            String json = objectMapper.writeValueAsString(coinsToTrack);
            Files.writeString(coinsFile, json);
        } catch (IOException e) {
            logger.error("Не удалось сохранить coins_to_track.json", e);
        }
    }

    public void addTrackedCoin(String coinId) {
        if (coinId != null) {
            String id = coinId.trim().toLowerCase();
            if (!coinsToTrack.contains(id)) {
                coinsToTrack.add(id);
                saveCoinsToFile();
                logger.info("Монета добавлена: {}", id);
            } else {
                logger.info("Монета уже отслеживается: {}", id);
            }
        }
    }

    public void removeTrackedCoin(String coinId) {
        if (coinId != null) {
            String id = coinId.trim().toLowerCase();
            if (coinsToTrack.remove(id)) {
                saveCoinsToFile();
                logger.info("Монета удалена: {}", id);
            } else {
                logger.warn("Монета {} не найдена в списке отслеживаемых", id);
            }
        }
    }

    public void initializeHistoricalDataIfNeeded(int daysBack) {
        long existingRecords = priceHistoryRepository.count();

        if (existingRecords < 100) {
            logger.info("Обнаружено мало данных ({} записей), загружаем исторические данные...", existingRecords);
            fetchAndSaveHistoricalData(daysBack);
        } else {
            logger.info("Данные уже существуют ({} записей), пропускаем загрузку истории", existingRecords);
        }
    }

    public void fetchAndSaveHistoricalData(int daysBack) {
        logger.info("Загрузка исторических данных за последние {} дней", daysBack);

        List<CoinPriceHistory> allHistoricalData = new ArrayList<>();
        int processedCoins = 0;

        for (String coinId : coinsToTrack) {
            try {
                logger.info("Загрузка исторических данных для монеты: {}", coinId);
                List<CoinPriceHistory> coinHistory = fetchCoinHistoricalData(coinId, daysBack);

                if (!coinHistory.isEmpty()) {
                    priceHistoryRepository.saveAll(coinHistory);
                    allHistoricalData.addAll(coinHistory);
                    logger.info("Сохранено {} записей для {}", coinHistory.size(), coinId);
                }

                processedCoins++;
                logger.info("Прогресс: {}/{} монет обработано", processedCoins, coinsToTrack.size());


                if (processedCoins < coinsToTrack.size()) {
                    Thread.sleep(25000);
                }

            } catch (Exception e) {
                logger.error("Ошибка при загрузке исторических данных для {}: {}", coinId, e.getMessage(), e);
            }
        }

        logger.info("Загрузка исторических данных завершена. Всего сохранено {} записей", allHistoricalData.size());
    }

    private List<CoinPriceHistory> fetchCoinHistoricalData(String coinId, int days) {
        Mono<JsonNode> response = webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/coins/{id}/market_chart")
                        .queryParam("vs_currency", "usd")
                        .queryParam("days", days)
                        .queryParam("interval", "daily")
                        .build(coinId))
                .retrieve()
                .bodyToMono(JsonNode.class)
                .timeout(Duration.ofSeconds(30))
                .retryWhen(Retry.fixedDelay(3, Duration.ofSeconds(25))
                        .filter(ex -> ex instanceof WebClientResponseException || ex instanceof java.util.concurrent.TimeoutException)
                );

        JsonNode data = response.block();
        if (data == null) {
            logger.warn("Не удалось получить исторические данные для {}", coinId);
            return new ArrayList<>();
        }

        List<CoinPriceHistory> historicalData = new ArrayList<>();

        try {
            JsonNode prices = data.get("prices");
            JsonNode marketCaps = data.get("market_caps");
            JsonNode totalVolumes = data.get("total_volumes");

            if (prices != null && prices.isArray()) {
                for (int i = 0; i < prices.size(); i++) {
                    JsonNode pricePoint = prices.get(i);
                    if (pricePoint != null && pricePoint.isArray() && pricePoint.size() >= 2) {

                        CoinPriceHistory history = new CoinPriceHistory();
                        history.setCoinId(coinId);
                        history.setSymbol(coinId);

                        long timestamp = pricePoint.get(0).asLong();
                        history.setTimestamp(Instant.ofEpochMilli(timestamp));

                        history.setPrice(pricePoint.get(1).asDouble());

                        if (marketCaps != null && marketCaps.isArray() && i < marketCaps.size()) {
                            JsonNode marketCapPoint = marketCaps.get(i);
                            if (marketCapPoint != null && marketCapPoint.isArray() && marketCapPoint.size() >= 2) {
                                history.setMarketCap(marketCapPoint.get(1).asDouble());
                            }
                        }

                        if (totalVolumes != null && totalVolumes.isArray() && i < totalVolumes.size()) {
                            JsonNode volumePoint = totalVolumes.get(i);
                            if (volumePoint != null && volumePoint.isArray() && volumePoint.size() >= 2) {
                                history.setVolume(volumePoint.get(1).asDouble());
                            }
                        }

                        historicalData.add(history);
                    }
                }
            }

            if (!historicalData.isEmpty()) {
                String symbol = getCoinSymbol(coinId);
                for (CoinPriceHistory history : historicalData) {
                    history.setSymbol(symbol);
                }
            }

        } catch (Exception e) {
            logger.error("Ошибка парсинга исторических данных для {}: {}", coinId, e.getMessage(), e);
        }

        return historicalData;
    }

    private String getCoinSymbol(String coinId) {
        try {
            Mono<JsonNode> response = webClient.get()
                    .uri(uriBuilder -> uriBuilder
                            .path("/coins/{id}")
                            .queryParam("localization", "false")
                            .queryParam("tickers", "false")
                            .queryParam("market_data", "false")
                            .queryParam("community_data", "false")
                            .queryParam("developer_data", "false")
                            .queryParam("sparkline", "false")
                            .build(coinId))
                    .retrieve()
                    .bodyToMono(JsonNode.class)
                    .timeout(Duration.ofSeconds(10));

            JsonNode data = response.block();
            if (data != null && data.has("symbol")) {
                return data.get("symbol").asText();
            }
        } catch (Exception e) {
            logger.warn("Не удалось получить символ для {}, используем ID как символ", coinId);
        }

        return coinId;
    }

    public List<CoinPriceHistory> fetchAndSaveCoinData() {
        logger.info("Загрузка и сохранение текущих цен для отслеживаемых монет");

        List<CoinPriceHistory> allHistory = new ArrayList<>();
        int batchSize = 5;
        int totalCoins = coinsToTrack.size();

        for (int i = 0; i < totalCoins; i += batchSize) {
            int end = Math.min(i + batchSize, totalCoins);
            List<String> batch = coinsToTrack.subList(i, end);
            logger.info("Загрузка батча монет {}", batch);

            try {
                List<CoinPriceHistory> batchHistory = fetchBatch(batch);
                priceHistoryRepository.saveAll(batchHistory);
                allHistory.addAll(batchHistory);
            } catch (Exception e) {
                logger.error("Ошибка при загрузке/сохранении батча монет {}: {}", batch, e.getMessage(), e);
            }

            if (end < totalCoins) {
                try {
                    Thread.sleep(30000);
                } catch (InterruptedException ignored) {}
            }
        }

        logger.info("Сохранено {} исторических записей", allHistory.size());
        return allHistory;
    }

    private List<CoinPriceHistory> fetchBatch(List<String> batch) {
        String ids = String.join(",", batch);

        Mono<List<CoinPriceHistory>> responseMono = webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/coins/markets")
                        .queryParam("vs_currency", "usd")
                        .queryParam("ids", ids)
                        .queryParam("order", "market_cap_desc")
                        .queryParam("per_page", batch.size())
                        .queryParam("page", 1)
                        .queryParam("sparkline", false)
                        .build())
                .retrieve()
                .bodyToMono(JsonNode.class)
                .timeout(Duration.ofSeconds(15))
                .retryWhen(Retry.fixedDelay(2, Duration.ofSeconds(3))
                        .filter(ex -> ex instanceof WebClientResponseException || ex instanceof java.util.concurrent.TimeoutException)
                )
                .map(jsonArray -> {
                    List<CoinPriceHistory> result = new ArrayList<>();
                    if (jsonArray != null && jsonArray.isArray()) {
                        for (JsonNode coinNode : jsonArray) {
                            CoinPriceHistory history = new CoinPriceHistory();
                            history.setCoinId(coinNode.get("id").asText());
                            history.setSymbol(coinNode.get("symbol").asText());
                            history.setPrice(coinNode.get("current_price").asDouble());
                            history.setVolume(coinNode.has("total_volume") ? coinNode.get("total_volume").asDouble() : null);
                            history.setMarketCap(coinNode.has("market_cap") ? coinNode.get("market_cap").asDouble() : null);
                            String lastUpdated = coinNode.get("last_updated").asText();
                            history.setTimestamp(lastUpdated != null ? Instant.parse(lastUpdated) : Instant.now());
                            result.add(history);
                        }
                    }
                    return result;
                });

        return responseMono.block();
    }



}