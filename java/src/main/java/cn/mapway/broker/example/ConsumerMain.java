package cn.mapway.broker.example;

import cn.mapway.broker.SatwayClient;
import cn.mapway.broker.SubscribeOptions;

public final class ConsumerMain {
    public static void main(String[] args) throws Exception {
        String broker = arg(args, "--broker", "127.0.0.1:7500");
        String topic = arg(args, "--topic", "cangling-test");
        String name = arg(args, "--name", "java-consumer");
        String token = arg(args, "--token", SatwayClient.authTokenFromEnv());
        try (SatwayClient client = SatwayClient.connect(broker, token, connected ->
                     System.out.println("connected to " + broker));
             var consumer = client.subscribe(
                     SubscribeOptions.topic(topic).name(name).build(),
                     message -> System.out.println(
                             "received | " + message.id() + " | " + message.topic() + " | " + message.payload()))) {
            System.out.println("subscribed consumer_id=" + consumer.consumerId());
            System.out.println("Press Ctrl+C to stop.");
            Thread main = Thread.currentThread();
            Runtime.getRuntime().addShutdownHook(new Thread(main::interrupt));
            try {
                main.join();
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static String arg(String[] args, String name, String fallback) {
        for (int i = 0; i < args.length - 1; i++) {
            if (name.equals(args[i])) {
                return args[i + 1];
            }
        }
        return fallback;
    }

    private ConsumerMain() {}
}
