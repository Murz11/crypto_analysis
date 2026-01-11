package org.example.crypto.service;

import com.fasterxml.jackson.databind.JsonNode;
import org.example.crypto.model.CoinPriceHistory;
import org.example.crypto.repository.CoinPriceHistoryRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class CoinGeckoServiceTest {

    private CoinPriceHistoryRepository priceHistoryRepository;
    private CoinGeckoService coinGeckoService;
    private WebClient.Builder webClientBuilder;
    private WebClient webClient;

    @BeforeEach
    void setUp() throws Exception {
        priceHistoryRepository = mock(CoinPriceHistoryRepository.class);


        webClientBuilder = mock(WebClient.Builder.class, RETURNS_DEEP_STUBS);
        webClient = mock(WebClient.class, RETURNS_DEEP_STUBS);

        coinGeckoService = new CoinGeckoService(priceHistoryRepository) {
            @Override
            @SuppressWarnings("unchecked")
            public void init() {
                try {
                    var field = CoinGeckoService.class.getDeclaredField("webClient");
                    field.setAccessible(true);
                    field.set(this, webClient);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                coinsToTrack = List.of("bitcoin", "ethereum");
            }
        };

        coinGeckoService.init();
    }


    @Test
    void addTrackedCoinShouldAddNewCoin() throws Exception {
        coinGeckoService.addTrackedCoin("dogecoin");
        assertTrue(coinGeckoService.getCoinsToTrack().contains("dogecoin"));
    }

    @Test
    void addTrackedCoinShouldNotAddDuplicate() throws Exception {
        int sizeBefore = coinGeckoService.getCoinsToTrack().size();
        coinGeckoService.addTrackedCoin("bitcoin");
        assertEquals(sizeBefore, coinGeckoService.getCoinsToTrack().size());
    }

    @Test
    void removeTrackedCoinShouldRemoveExisting() throws Exception {
        coinGeckoService.addTrackedCoin("dogecoin");
        coinGeckoService.removeTrackedCoin("dogecoin");
        assertFalse(coinGeckoService.getCoinsToTrack().contains("dogecoin"));
    }

    @Test
    void removeTrackedCoinNonExistentShouldDoNothing() throws Exception {
        int sizeBefore = coinGeckoService.getCoinsToTrack().size();
        coinGeckoService.removeTrackedCoin("nonexistent");
        assertEquals(sizeBefore, coinGeckoService.getCoinsToTrack().size());
    }

    @Test
    void initializeHistoricalDataIfNeededShouldFetchWhenFewRecords() {
        when(priceHistoryRepository.count()).thenReturn(50L);

        CoinGeckoService spy = Mockito.spy(coinGeckoService);
        doNothing().when(spy).fetchAndSaveHistoricalData(anyInt());

        spy.initializeHistoricalDataIfNeeded(30);
        verify(spy, times(1)).fetchAndSaveHistoricalData(30);
    }

    @Test
    void initializeHistoricalDataIfNeededShouldSkipWhenEnoughRecords() {
        when(priceHistoryRepository.count()).thenReturn(200L);

        CoinGeckoService spy = Mockito.spy(coinGeckoService);
        doNothing().when(spy).fetchAndSaveHistoricalData(anyInt());

        spy.initializeHistoricalDataIfNeeded(30);
        verify(spy, never()).fetchAndSaveHistoricalData(anyInt());
    }

    @Test
    void fetchAndSaveCoinDataShouldSaveAllFetchedCoins() {
        CoinPriceHistory mockHistory = new CoinPriceHistory();
        mockHistory.setCoinId("bitcoin");
        mockHistory.setPrice(10000.0);

        CoinGeckoService spy = Mockito.spy(coinGeckoService);
        doReturn(List.of(mockHistory)).when(spy).fetchBatch(anyList());

        List<CoinPriceHistory> result = spy.fetchAndSaveCoinData();

        assertEquals(1, result.size());
        verify(priceHistoryRepository, times(1)).saveAll(anyList());
    }

    @Test
    void fetchBatchShouldHandleEmptyResponseGracefully() {
        CoinGeckoService spy = Mockito.spy(coinGeckoService);
        doReturn(new ArrayList<CoinPriceHistory>()).when(spy).fetchBatch(anyList());

        List<CoinPriceHistory> result = spy.fetchAndSaveCoinData();
        assertTrue(result.isEmpty());
    }

    @Test
    void getCoinSymbolShouldReturnCoinIdIfException() {
        CoinGeckoService spy = Mockito.spy(coinGeckoService);
        doThrow(RuntimeException.class).when(spy).getCoinSymbol(anyString());

        String symbol = spy.getCoinSymbol("bitcoin");
        assertEquals("bitcoin", symbol);
    }
}
