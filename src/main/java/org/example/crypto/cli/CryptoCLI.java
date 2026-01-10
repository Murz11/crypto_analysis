package org.example.crypto.cli;

import org.example.crypto.service.AutoUpdateService;
import org.example.crypto.service.CoinGeckoService;
import org.example.crypto.service.PriceHistoryService;
import org.example.crypto.spark.SparkRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import picocli.CommandLine.Command;

import java.util.List;
import java.util.Scanner;
import java.util.concurrent.Callable;

@Component
@Command(name = "crypto-cli", mixinStandardHelpOptions = true)
public class CryptoCLI implements Callable<Integer> {

    @Autowired
    private CoinGeckoService coinGeckoService;

    @Autowired
    private PriceHistoryService priceHistoryService;

    @Autowired
    private AutoUpdateService autoUpdateService;

    @Autowired
    private SparkRunner sparkRunner;

    private final Scanner scanner = new Scanner(System.in);

    @Override
    public Integer call() {
        boolean exit = false;

        while (!exit) {
            System.out.println("\n=== CryptoTracker CLI ===");
            System.out.println("1. Собрать данные монет");
            System.out.println("2. Показать список отслеживаемых монет");
            System.out.println("3. Добавить монету для отслеживания");
            System.out.println("4. Удалить монету из отслеживания");
            System.out.println("5. Показать последнюю цену каждой монеты");
            System.out.println("6. Запустить Spark-анализ");
            System.out.println("7. Включить автообновление данных");
            System.out.println("8. Отключить автообновление");
            System.out.println("9. Загрузить исторические данные");
            System.out.println("0. Выход");
            System.out.print("Выберите действие: ");

            String choice = scanner.nextLine().trim();

            switch (choice) {
                case "1":
                    System.out.println("Сбор данных");
                    coinGeckoService.fetchAndSaveCoinData();
                    System.out.println("Данные собраны");
                    break;

                case "2":
                    System.out.println("Отслеживаемые монеты:");
                    List<String> coins = coinGeckoService.getCoinsToTrack();
                    if (coins.isEmpty()) {
                        System.out.println("Список пуст");
                    } else {
                        coins.forEach(System.out::println);
                    }
                    break;


                case "3":
                    System.out.print("Введите ID монеты для добавления: ");
                    String addCoin = scanner.nextLine().trim();
                    if (!addCoin.isEmpty()) {
                        coinGeckoService.addTrackedCoin(addCoin);
                    }
                    break;

                case "4":
                    System.out.print("Введите ID монеты для удаления: ");
                    String removeCoin = scanner.nextLine().trim();
                    if (!removeCoin.isEmpty()) {
                        coinGeckoService.removeTrackedCoin(removeCoin);;
                    }
                    break;

                case "5":
                    System.out.println("Последние цены монет:");
                    List<String> trackedCoins = priceHistoryService.getAllCoinIds();
                    if (trackedCoins.isEmpty()) {
                        System.out.println("Список пуст");
                    } else {
                        for (String coinId : trackedCoins) {
                            var lastPrice = priceHistoryService.getLastPriceForCoin(coinId);
                            if (lastPrice != null) {
                                System.out.printf("%s: $%.2f (%s)%n",
                                        coinId,
                                        lastPrice.getPrice(),
                                        lastPrice.getTimestamp());
                            } else {
                                System.out.printf("%s: данных нет%n", coinId);
                            }
                        }
                    }
                    break;

                case "6":
                    sparkRunner.runSparkAnalysis();
                    break;

                case "7":
                    autoUpdateService.startAutoUpdate();
                    break;

                case "8":
                    autoUpdateService.stopAutoUpdate();
                    break;

                case "9":
                    handleHistoricalData();
                    break;

                case "0":
                    exit = true;
                    System.out.println("Выход");
                    break;

                default:
                    System.out.println("Некорректный выбор, попробуйте снова");
            }
        }

        return 0;
    }

    private void handleHistoricalData() {
        System.out.println("\n=== Загрузка исторических данных ===");
        System.out.println("1. Автоматически (только если данных мало)");
        System.out.println("2. Принудительно за последние 90 дней");
        System.out.println("3. Принудительно с выбором количества дней");
        System.out.print("Выберите вариант: ");

        String choice = scanner.nextLine().trim();

        switch (choice) {
            case "1":
                System.out.println("Проверка и загрузка исторических данных");
                coinGeckoService.initializeHistoricalDataIfNeeded(90);
                System.out.println("Автоматическая загрузка завершена");
                break;

            case "2":
                System.out.println("Загрузка исторических данных за 90 дней");
                coinGeckoService.fetchAndSaveHistoricalData(90);
                System.out.println("Исторические данные за 90 дней загружены");
                break;

            case "3":
                System.out.print("Введите количество дней для загрузки: ");
                try {
                    int days = Integer.parseInt(scanner.nextLine().trim());
                    if (days > 0 && days <= 365) {
                        System.out.println("Загрузка исторических данных за " + days + " дней");
                        coinGeckoService.fetchAndSaveHistoricalData(days);
                        System.out.println("Исторические данные за " + days + " дней загружены");
                    } else {
                        System.out.println("Некорректное количество дней (1-365)");
                    }
                } catch (NumberFormatException e) {
                    System.out.println("Некорректный формат числа");
                }
                break;

            default:
                System.out.println("Некорректный выбор");
        }
    }
}