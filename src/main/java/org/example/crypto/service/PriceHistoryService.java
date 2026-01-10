package org.example.crypto.service;

import org.example.crypto.model.CoinPriceHistory;
import org.example.crypto.repository.CoinPriceHistoryRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

@Service
public class PriceHistoryService {

    private final CoinPriceHistoryRepository coinPriceHistoryRepository;

    @Autowired
    public PriceHistoryService(CoinPriceHistoryRepository coinPriceHistoryRepository) {
        this.coinPriceHistoryRepository = coinPriceHistoryRepository;
    }

    public List<String> getAllCoinIds() {
        return coinPriceHistoryRepository.findAllDistinctCoinIds();
    }

    public CoinPriceHistory getLastPriceForCoin(String coinId) {
        List<CoinPriceHistory> prices = coinPriceHistoryRepository
                .findByCoinIdOrderByTimestampDesc(coinId, PageRequest.of(0, 1));
        return prices.isEmpty() ? null : prices.get(0);
    }

}
