package org.example.crypto.service;

import org.example.crypto.spark.SparkBatchProcessor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.concurrent.ScheduledExecutorService;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class AutoUpdateServiceTest {

    private CoinGeckoService coinGeckoService;
    private SparkBatchProcessor sparkBatchProcessor;
    private AutoUpdateService autoUpdateService;

    @BeforeEach
    void setUp() {
        coinGeckoService = mock(CoinGeckoService.class);
        sparkBatchProcessor = mock(SparkBatchProcessor.class);
        autoUpdateService = new AutoUpdateService(coinGeckoService, sparkBatchProcessor);
        autoUpdateService.init();
    }


    private ScheduledExecutorService getScheduler() throws Exception {
        var field = AutoUpdateService.class.getDeclaredField("scheduler");
        field.setAccessible(true);
        return (ScheduledExecutorService) field.get(autoUpdateService);
    }
    @Test
    void startAutoUpdateShouldEnableAutoUpdateAndScheduleTask() throws Exception {
        autoUpdateService.startAutoUpdate();

        assertTrue(autoUpdateService.isAutoUpdateEnabled());
        assertNotNull(getScheduler());
        assertFalse(getScheduler().isShutdown());
    }

    @Test
    void stopAutoUpdateShouldDisableAutoUpdateAndShutdownScheduler() throws Exception {
        autoUpdateService.startAutoUpdate();
        autoUpdateService.stopAutoUpdate();

        assertFalse(autoUpdateService.isAutoUpdateEnabled());
        assertTrue(getScheduler().isShutdown());
    }

    @Test
    void getStatusShouldReturnCorrectValue() {
        assertEquals("Остановлено", autoUpdateService.getStatus());
        autoUpdateService.startAutoUpdate();
        assertEquals("Активно", autoUpdateService.getStatus());
    }
    @Test
    void startAutoUpdateWhenAlreadyStartedShouldNotThrowAndLogWarning() {
        autoUpdateService.startAutoUpdate();
        autoUpdateService.startAutoUpdate(); // второй вызов
        assertTrue(autoUpdateService.isAutoUpdateEnabled());
    }

    @Test
    void stopAutoUpdateWhenAlreadyStoppedShouldNotThrowAndLogWarning() throws Exception {
        autoUpdateService.stopAutoUpdate(); // первый вызов на остановленном сервисе
        assertFalse(autoUpdateService.isAutoUpdateEnabled());
    }

    @Test
    void updateDataAndSparkShouldHandleExceptionGracefully() {
        AutoUpdateService spyService = Mockito.spy(autoUpdateService);

        doThrow(new RuntimeException("API Error"))
                .when(coinGeckoService).fetchAndSaveCoinData();

        // вызываем приватный метод через reflection
        try {
            var method = AutoUpdateService.class.getDeclaredMethod("updateDataAndSpark");
            method.setAccessible(true);
            method.invoke(spyService);
        } catch (Exception e) {
            fail("Метод updateDataAndSpark должен обрабатывать исключения, а не выбрасывать их наружу");
        }
    }
    @Test
    void updateDataAndSparkShouldCallCoinGeckoAndSpark() throws Exception {
        AutoUpdateService spyService = Mockito.spy(autoUpdateService);

        // вызываем приватный метод через reflection
        var method = AutoUpdateService.class.getDeclaredMethod("updateDataAndSpark");
        method.setAccessible(true);
        method.invoke(spyService);

        verify(coinGeckoService, times(1)).fetchAndSaveCoinData();
        verify(sparkBatchProcessor, times(1)).runDailyAnalysis();
    }

}
