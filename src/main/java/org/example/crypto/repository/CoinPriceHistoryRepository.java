package org.example.crypto.repository;

import org.example.crypto.model.CoinPriceHistory;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface CoinPriceHistoryRepository extends JpaRepository<CoinPriceHistory, Long> {


    List<CoinPriceHistory> findByCoinIdOrderByTimestampDesc(String coinId, Pageable pageable);


    @Query("SELECT DISTINCT c.coinId FROM CoinPriceHistory c")
    List<String> findAllDistinctCoinIds();
}
