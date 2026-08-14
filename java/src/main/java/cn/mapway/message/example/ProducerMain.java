package cn.mapway.message.example;

import cn.mapway.message.SatwayClient;
import cn.mapway.message.SendResult;

public final class ProducerMain {
    public static void main(String[] args) {
        String broker = arg(args, "--broker", "127.0.0.1:7500");
        String topic = arg(args, "--topic", "cangling-test");
        String text = arg(args, "--text", "hello");
        int count = Integer.parseInt(arg(args, "--count", "1"));
        try (SatwayClient client = SatwayClient.connect(broker)) {
            for (int i = 0; i < count; i++) {
                String payload = count == 1 ? text : text + "-" + i;
                SendResult result = client.send(topic, payload);
                System.out.println("sent " + result);
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

    private ProducerMain() {}
}
