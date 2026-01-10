package org.example.crypto;

import org.example.crypto.cli.CryptoCLI;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import picocli.CommandLine;
import org.springframework.context.ApplicationContext;

@SpringBootApplication
public class CryptoApplication {
    public static void main(String[] args) {
        ApplicationContext ctx = SpringApplication.run(CryptoApplication.class, args);
        int exitCode = new CommandLine(ctx.getBean(CryptoCLI.class)).execute(args);
        System.exit(exitCode);
    }
}
