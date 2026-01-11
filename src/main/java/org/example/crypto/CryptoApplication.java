package org.example.crypto;

import org.example.crypto.cli.CryptoCLI;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;
import picocli.CommandLine;

@SpringBootApplication
public class CryptoApplication {

    public static void main(String[] args) {
        ApplicationContext ctx = SpringApplication.run(CryptoApplication.class, args);

        Environment env = ctx.getEnvironment();
        if (env.acceptsProfiles("local")) {
            int exitCode = new CommandLine(ctx.getBean(CryptoCLI.class)).execute(args);
            System.exit(exitCode);
        }
    }
}
