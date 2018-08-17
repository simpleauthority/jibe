package com.algorithmjunkie.jibe;

import com.algorithmjunkie.mc.konfig.core.database.AuthAdapter;
import com.google.common.eventbus.EventBus;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;

public final class Jibe {
    private final String channel;
    private final JedisPool pool;
    private final EventBus eventBus;
    private final Gson gson;

    private Thread handlerThread;

    public Jibe(String channel, AuthAdapter authentication) {
        this.channel = channel;
        pool = new JedisPool(new JedisPoolConfig(), authentication.getHostName(), authentication.getPort());
        eventBus = new EventBus();
        gson = new GsonBuilder().enableComplexMapKeySerialization().serializeNulls().create();

        handlerThread = new Thread(new RedisPubSubListener());
        handlerThread.start();
    }

    public final void post(JibeMessage message) {
        try (Jedis jedis = pool.getResource()) {
            jedis.publish(
                    channel,
                    gson.toJson(message, message.getClass())
            );
        }
    }

    public final void shutdown() {
        handlerThread.interrupt();
    }

    public final EventBus getEventBus() {
        return eventBus;
    }

    @SuppressWarnings("unchecked")
    // We can safely assume the type here.
    private void dispatch(String incoming) {
        final JibeMessage base = gson.fromJson(incoming, JibeMessage.class);
        try {
            eventBus.post(gson.fromJson(
                    incoming,
                    (Class<? extends JibeMessage>) Class.forName(base.getClassName())
            ));
        } catch (ClassNotFoundException ex) {
            ex.printStackTrace();
        }
    }

    public static class JibeMessage {
        private final String className;

        public JibeMessage() {
            className = getClass().getName();
        }

        String getClassName() {
            return className;
        }
    }

    private final class RedisPubSubListener implements Runnable {
        private RedisPubSubHandler handler;

        @Override
        public void run() {
            boolean didBreak = false;

            try (Jedis jedis = pool.getResource()) {
                try {
                    jedis.subscribe(handler = new RedisPubSubHandler(), channel);
                } catch (Exception ex) {
                    ex.printStackTrace();
                    didBreak = true;
                    try {
                        handler.unsubscribe();
                    } catch (Exception ex2) {
                        ex2.printStackTrace();
                    }
                }
            }

            if (didBreak) {
                System.err.println("Error occurred inside Redis handler. Restarting now.");
                run();
            }
        }
    }

    private final class RedisPubSubHandler extends JedisPubSub {
        @Override
        public void onMessage(String channel, String message) {
            if (!Jibe.this.channel.equals(channel)) {
                return;
            }

            dispatch(message);
        }
    }
}
