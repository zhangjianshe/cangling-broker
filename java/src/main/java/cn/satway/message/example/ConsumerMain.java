package cn.satway.message.example;

import cn.satway.message.MessageClient;
import cn.satway.message.SubscribeOptions;

public final class ConsumerMain {
    public static void main(String[] args) throws Exception {
        String broker = arg(args, "--broker", "127.0.0.1:7500");
        String topic = arg(args, "--topic", "cangling-test");
        String listen = arg(args, "--listen", "127.0.0.1:8080");
        String callback = arg(args, "--callback-url", "");
        int colon = listen.lastIndexOf(':');
        if (colon <= 0) {
            throw new IllegalArgumentException("--listen must be host:port");
        }
        String host = listen.substring(0, colon);
        int port = Integer.parseInt(listen.substring(colon + 1));
        SubscribeOptions.Builder options = SubscribeOptions.topic(topic).listen(host, port);
        if (!callback.isBlank()) {
            options.callbackUrl(callback);
        }
        try (MessageClient client = MessageClient.connect(broker);
             var consumer = client.subscribe(options.build(), message ->
                     System.out.println("received | " + message.id() + " | " + message.topic() + " | " + message.payload()))) {
            System.out.println("subscribed consumer_id=" + consumer.consumerId() + " callback=" + consumer.callbackUrl());
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
