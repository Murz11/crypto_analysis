package org.example.crypto.service;

import org.example.crypto.model.CoinPriceHistory;
import org.example.crypto.repository.CoinPriceHistoryRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.springframework.data.domain.PageRequest;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class PriceHistoryServiceTest {

    private CoinPriceHistoryRepository repository;
    private PriceHistoryService service;

    @BeforeEach
    void setUp() {
        repository = mock(CoinPriceHistoryRepository.class);
        service = new PriceHistoryService(repository);
    }

    @Test
    void getAllCoinIdsShouldReturnListFromRepository() {
        List<String> mockIds = List.of("bitcoin", "ethereum", "dogecoin");
        when(repository.findAllDistinctCoinIds()).thenReturn(mockIds);

        List<String> result = service.getAllCoinIds();
        assertEquals(3, result.size());
        assertTrue(result.contains("bitcoin"));
        verify(repository, times(1)).findAllDistinctCoinIds();
    }

    @Test
    void getAllCoinIdsShouldReturnEmptyListIfRepositoryEmpty() {
        when(repository.findAllDistinctCoinIds()).thenReturn(List.of());

        List<String> result = service.getAllCoinIds();
        assertTrue(result.isEmpty());
        verify(repository, times(1)).findAllDistinctCoinIds();
    }

    @Test
    void getLastPriceForCoinShouldReturnLatestRecord() {
        CoinPriceHistory history = new CoinPriceHistory();
        history.setCoinId("bitcoin");
        history.setPrice(50000.0);
        history.setTimestamp(Instant.now());

        when(repository.findByCoinIdOrderByTimestampDesc(eq("bitcoin"), any(PageRequest.class)))
                .thenReturn(List.of(history));

        CoinPriceHistory result = service.getLastPriceForCoin("bitcoin");
        assertNotNull(result);
        assertEquals("bitcoin", result.getCoinId());
        assertEquals(50000, result.getPrice());
        verify(repository, times(1)).findByCoinIdOrderByTimestampDesc(eq("bitcoin"), any(PageRequest.class));
    }

    @Test
    void getLastPriceForCoinShouldReturnNullIfNoRecords() {
        when(repository.findByCoinIdOrderByTimestampDesc(eq("bitcoin"), any(PageRequest.class)))
                .thenReturn(List.of());

        CoinPriceHistory result = service.getLastPriceForCoin("bitcoin");
        assertNull(result);
        verify(repository, times(1)).findByCoinIdOrderByTimestampDesc(eq("bitcoin"), any(PageRequest.class));
    }

    @Test
    void getLastPriceForCoinShouldHandleNullCoinIdGracefully() {
        when(repository.findByCoinIdOrderByTimestampDesc(eq(null), any(PageRequest.class)))
                .thenReturn(List.of());

        CoinPriceHistory result = service.getLastPriceForCoin(null);
        assertNull(result);
        verify(repository, times(1)).findByCoinIdOrderByTimestampDesc(eq(null), any(PageRequest.class));
    }
}
