package ru.iitdgroup.internal;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.*;
import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import javax.cache.spi.CachingProvider;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Демонстратор работы с кэшом
 */
@SuppressWarnings({"squid:S106"})
public class CacheExamples {

    private final static String CACHE_NAME = "simpleCache";

    @SuppressWarnings({"squid:S2095"})
    public static void main(String[] args) {


        try (CachingProvider cachingProvider = Caching.getCachingProvider()) {
            CacheManager cacheManager = getCacheManager(cachingProvider);
            MutableConfiguration<String, byte[]> config
                    = new MutableConfiguration<>();


            Cache<String, byte[]> cache;

            cache = cacheManager.getCache(CACHE_NAME);

            if (cache == null) {
                cache = cacheManager.createCache(CACHE_NAME, config);
            }


            registerListener(cache);

            cache.put("key1", "value1".getBytes(UTF_8));
            cache.put("key2", "value2".getBytes(UTF_8));

            EntryProcessor<String, byte[], byte[]> ep = new ByteEntryProcessor();

            //FIXME: тут нужно бы открыть баг в IDEA - противоречащие хинты
            cache.invoke("key1", ep, (Object) "New1".getBytes(UTF_8));
            cache.invoke("key2", ep, (Object) "New2".getBytes(UTF_8));
            cache.invoke("key3", ep, (Object) "New3".getBytes(UTF_8));

            System.out.println("\n\n\n------------- listing cache -----------------");
            cache.forEach(entry -> System.out.println(entry.getKey() + " : " + new String(entry.getValue())));
        }
    }

    /**
     * Регистрация подписчика на события в кэше
     * @param cache кэш, для которого регистрировать
     */
    private static void registerListener(Cache<String,byte[]> cache) {
        // create the EntryListener
        //ByteEntryUpdateListener clientListener = new ByteEntryUpdateListener();

        // using our listener, let's create a configuration
        CacheEntryListenerConfiguration<String, byte[]> conf = new MutableCacheEntryListenerConfiguration<>(
                FactoryBuilder.factoryOf(ByteEntryUpdateListener.class),
                null, true, false);

        // register it to the cache at run-time
        cache.registerCacheEntryListener(conf);
    }


    private static CacheManager getCacheManager(CachingProvider cachingProvider) {

        // Get or create a cache manager.
        CacheManager cacheMgr = Caching.getCachingProvider().getCacheManager(
                Paths.get("client-config.xml").toUri(), null);

        return cacheMgr;
    }

    public static class ByteEntryProcessor implements EntryProcessor<String, byte[], byte[]>, Serializable {
        /**
         * Обработка одной записи или создание новой
         *
         * @param entry     что получили из кэша
         * @param arguments byte[] один аргумент - новое значение
         * @return byte[] актуальное значение ключа
         * @throws EntryProcessorException в случае, если нужно создавать новую запись, а значение для неё не передали
         */
        @Override
        public byte[] process(MutableEntry<String, byte[]> entry, Object... arguments) throws EntryProcessorException {
            System.out.println("Processing key: " + entry.getKey());
            String newValue = null;
            if (arguments.length > 0 && arguments[0].getClass() == byte[].class) {
                newValue = new String((byte[]) arguments[0]);
                System.out.println("byte arr arg: " + newValue);
            } else {
                System.out.println(arguments.length > 0 ? "unknown arg(s)" : "no args");
            }

            byte[] valueBytes = entry.getValue();
            if (valueBytes == null) {
                //нет такого ключа: делаем новый
                if (newValue != null) {
                    System.out.println("Creating new key");
                    valueBytes = newValue.getBytes(UTF_8);
                    entry.setValue(valueBytes);
                } else {
                    throw new EntryProcessorException("Null key and no new key value provided");
                }
            }
            System.out.println("Processed value: " + new String(valueBytes));
            return valueBytes;
        }
    }

    public static class ByteEntryUpdateListener implements CacheEntryUpdatedListener<String, byte[]>, CacheEntryCreatedListener<String,byte[]> {
        @Override
        public void onUpdated(Iterable<CacheEntryEvent<? extends String, ? extends byte[]>> cacheEntryEvents) throws CacheEntryListenerException {
            for (CacheEntryEvent<? extends String, ? extends byte[]> event : cacheEntryEvents) {
                System.out.println(String.format("Listener ->> Updated: key = %s, value = %s",
                        event.getKey(),
                        new String( event.getValue(), UTF_8)));
            }
        }

        @Override
        public void onCreated(Iterable<CacheEntryEvent<? extends String, ? extends byte[]>> cacheEntryEvents) throws CacheEntryListenerException {
            for (CacheEntryEvent<? extends String, ? extends byte[]> event : cacheEntryEvents) {
                System.out.println("Listener ->> created");
            }
        }
    }

    public static class LoggingEntryListenerFactory implements Factory<ByteEntryUpdateListener> {
        @Override
        public ByteEntryUpdateListener create() {
            return new ByteEntryUpdateListener();
        }
    }

}
