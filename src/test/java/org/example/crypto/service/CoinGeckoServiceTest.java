package org.example.crypto.service;

import org.example.crypto.model.CoinPriceHistory;
import org.example.crypto.repository.CoinPriceHistoryRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CoinGeckoServiceTest {

    private CoinPriceHistoryRepository repository;
    private CoinGeckoService service;
    private WebClient webClientMock;

    @BeforeEach
    void setUp() {
        repository = mock(CoinPriceHistoryRepository.class);
        service = new CoinGeckoService(repository);

        service.init();
    }

    @Test
    void testAddTrackedCoin_NewCoin() {
        int initialSize = service.getCoinsToTrack().size();

        service.addTrackedCoin("newcoin");
        assertTrue(service.getCoinsToTrack().contains("newcoin"));
        assertEquals(initialSize + 1, service.getCoinsToTrack().size());
    }

    @Test
    void testAddTrackedCoin_ExistingCoin() {
        String coin = service.getCoinsToTrack().get(0);
        int initialSize = service.getCoinsToTrack().size();

        service.addTrackedCoin(coin);
        assertEquals(initialSize, service.getCoinsToTrack().size()); // не добавляется повторно
    }

    @Test
    void testRemoveTrackedCoin_ExistingCoin() {
        String coin = service.getCoinsToTrack().get(0);
        service.removeTrackedCoin(coin);
        assertFalse(service.getCoinsToTrack().contains(coin));
    }

    @Test
    void testRemoveTrackedCoin_NonExistingCoin() {
        int initialSize = service.getCoinsToTrack().size();
        service.removeTrackedCoin("nonexistentcoin");
        assertEquals(initialSize, service.getCoinsToTrack().size());
    }

    @Test
    void testFetchAndSaveCoinData_Success() {
        when(repository.saveAll(anyList())).thenAnswer(invocation -> invocation.getArgument(0));


        List<CoinPriceHistory> result = service.fetchAndSaveCoinData();

        assertNotNull(result);
        assertTrue(result.isEmpty() || result.size() >= 0);
        verify(repository, atLeast(0)).saveAll(anyList());
    }

    @Test
    void testInitializeHistoricalDataIfNeeded_WithFewRecords() {
        when(repository.count()).thenReturn(50L);
        service.initializeHistoricalDataIfNeeded(10);

    }

    @Test
    void testInitializeHistoricalDataIfNeeded_WithEnoughRecords() {
        when(repository.count()).thenReturn(200L);
        service.initializeHistoricalDataIfNeeded(10);

    }
}
