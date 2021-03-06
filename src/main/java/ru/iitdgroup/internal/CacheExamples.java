package ru.iitdgroup.internal;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.FactoryBuilder.SingletonFactory;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriter;
import javax.cache.integration.CacheWriterException;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import javax.cache.spi.CachingProvider;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Демонстратор работы с кэшом
 *
 * <p>Порядок запуска: 1 - Ignite Server, 2 - CacheExamplesTest</p>
 * <p>
 * <li><ul>Server output</ul></li>
 * <pre>
 *  Cache loader ->> load data for key NON_EXISTENT_KEY
 *  Cache writer ->> write, key: key1, value: value1
 *  Cache writer ->> write, key: key2, value: value2
 *  Processing key: key1
 *  byte arr arg: New App data for key 1
 *  Processed value: value1
 *  Cache writer ->> write, key: key1, value: New App data for key 1
 *  Processing key: key2
 *  byte arr arg: New App data for key 2
 *  Processed value: value2
 *  Cache writer ->> write, key: key2, value: New App data for key 2
 *  Cache loader ->> load data for key NEW_KEY
 *  Processing key: NEW_KEY
 *  byte arr arg: New App data for key NEW_KEY
 *  Processed value: DB data for key NEW_KEY
 *  Cache writer ->> write, key: NEW_KEY, value: New App data for key NEW_KEY
 *  </pre>
 * </p>
 * <p>
 * <li><ul>Client output</ul></li>
 * <pre>
 * ByteEntryCacheLoader created
 * ByteEntryCacheWriter created
 *
 * App ->> createCache
 *
 * App ->> registerListener
 * ByteEntryUpdateListener created
 *
 * App ->> checking non - existent key
 * App -->> Getting non-existent cache entry: key NON_EXISTENT_KEY, value: DB data for key NON_EXISTENT_KEY
 *
 * App ->> putting two keys
 *
 * App ->> creating entry processor
 * ByteEntryProcessor created
 *
 * App ->> processing existing keys
 * Listener ->> created key key1, value value1
 * Listener ->> created key key2, value value2
 * Listener ->> Updated: key = key1, value = New App data for key 1
 *
 * App ->> processing new key
 * Listener ->> Updated: key = key2, value = New App data for key 2
 *
 * App ->> listing cache
 * Listener ->> created key NEW_KEY, value New App data for key NEW_KEY
 * key1 : New App data for key 1
 * key2 : New App data for key 2
 * NON_EXISTENT_KEY : DB data for key NON_EXISTENT_KEY
 * NEW_KEY : New App data for key NEW_KEY
 * </pre>
 * </p>
 */
@SuppressWarnings({"squid:S106"})
public class CacheExamples {

    private final static String CACHE_NAME = "simpleCache";

    @SuppressWarnings({"squid:S2095"})
    public static void main(String[] args) {


        try (CachingProvider cachingProvider = Caching.getCachingProvider()) {
            CacheManager cacheManager = getCacheManager();
            MutableConfiguration<String, byte[]> config
                    = new MutableConfiguration<>();

            SingletonFactory<ByteEntryCacheLoader> loaderFactory = new SerializableSingletonFactory<>(new ByteEntryCacheLoader());
            SingletonFactory<ByteEntryCacheWriter> writerFactory = new SerializableSingletonFactory<>(new ByteEntryCacheWriter());

            config
                    .setWriteThrough(true)
                    .setCacheWriterFactory(writerFactory)
                    .setReadThrough(true)
                    .setCacheLoaderFactory(loaderFactory)
            ;

            Cache<String, byte[]> cache;

            System.out.println("\nApp ->> createCache");
            cache = cacheManager.getCache(CACHE_NAME);
            if (cache == null) {
                cache = cacheManager.createCache(CACHE_NAME, config);
            }
            cache.clear();

            System.out.println("\nApp ->> registerListener");
            registerListener(cache);

            System.out.println("\nApp ->> checking non - existent key");
            final String nonExistentKey = "NON_EXISTENT_KEY";
            final byte[] data = cache.get(nonExistentKey);
            System.out.println(String.format("App -->> Getting non-existent cache entry: key %s, value: %s",
                    nonExistentKey,
                    new String(data == null ? "NULL".getBytes(UTF_8) : data, UTF_8)));

            System.out.println("\nApp ->> putting two keys");
            cache.put("key1", "value1".getBytes(UTF_8));
            cache.put("key2", "value2".getBytes(UTF_8));

            System.out.println("\nApp ->> creating entry processor");
            EntryProcessor<String, byte[], byte[]> ep = new ByteEntryProcessor();

            //FIXME: тут нужно бы открыть баг в IDEA - противоречащие хинты
            System.out.println("\nApp ->> processing existing keys");
            //noinspection RedundantCast
            cache.invoke("key1", ep, (Object) "New App data for key 1".getBytes(UTF_8));
            //noinspection RedundantCast
            cache.invoke("key2", ep, (Object) "New App data for key 2".getBytes(UTF_8));

            System.out.println("\nApp ->> processing new key");
            //noinspection RedundantCast
            cache.invoke("NEW_KEY", ep, (Object) "New App data for key NEW_KEY".getBytes(UTF_8));

            System.out.println("\nApp ->> listing cache");
            cache.forEach(entry -> System.out.println(entry.getKey() + " : " + new String(entry.getValue())));
        }
    }

    /**
     * Регистрация подписчика на события в кэше
     *
     * @param cache кэш, для которого регистрировать
     */
    private static void registerListener(Cache<String, byte[]> cache) {
        SingletonFactory<ByteEntryUpdateListener> listenerFactory = new SerializableSingletonFactory<>(new ByteEntryUpdateListener());

        // using our listener, let's create a configuration
        CacheEntryListenerConfiguration<String, byte[]> conf = new MutableCacheEntryListenerConfiguration<>(
                listenerFactory,
                null, true, false);


        // register it to the cache at run-time
        cache.registerCacheEntryListener(conf);

    }

    /**
     * Класс для обработки записей на стороне сервера кэша.
     * <p>Сериализуется и уезжает работать на стороне сервера!</p>
     */
    public static class ByteEntryProcessor implements EntryProcessor<String, byte[], byte[]>, Serializable {
        public ByteEntryProcessor() {
            System.out.println(this.getClass().getSimpleName() + " created");
        }

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
            String newValue;
            if (arguments.length > 0 && arguments[0].getClass() == byte[].class) {
                newValue = new String((byte[]) arguments[0]);
                System.out.println("byte arr arg: " + newValue);
            } else {
                System.out.println(arguments.length > 0 ? "unknown arg(s)" : "no args");
                return null;
            }

            byte[] valueBytes = entry.getValue();
            if (valueBytes == null) {
                //нет такого ключа: делаем новый
                System.out.println("Creating new key");
                valueBytes = newValue.getBytes(UTF_8);
                entry.setValue(valueBytes);
            } else {
                entry.setValue(newValue.getBytes());
            }
            System.out.println("Processed value: " + new String(valueBytes));
            return valueBytes;
        }
    }

    /**
     * Класс для обработки событий изменения записей в кэше.
     * <p>Работает ТОЛЬКО на get/put/... и НЕ вызывается при обработке с помощью Entry processor</p>
     */
    public static class ByteEntryUpdateListener implements CacheEntryUpdatedListener<String, byte[]>, CacheEntryCreatedListener<String, byte[]>, Serializable {
        public ByteEntryUpdateListener() {
            System.out.println(this.getClass().getSimpleName() + " created");
        }

        @Override
        public void onUpdated(Iterable<CacheEntryEvent<? extends String, ? extends byte[]>> cacheEntryEvents) throws CacheEntryListenerException {
            for (CacheEntryEvent<? extends String, ? extends byte[]> event : cacheEntryEvents) {
                System.out.println(String.format("Listener ->> Updated: key = %s, value = %s",
                        event.getKey(),
                        new String(event.getValue(), UTF_8)));
            }
        }

        @Override
        public void onCreated(Iterable<CacheEntryEvent<? extends String, ? extends byte[]>> cacheEntryEvents) throws CacheEntryListenerException {
            for (CacheEntryEvent<? extends String, ? extends byte[]> event : cacheEntryEvents) {
                System.out.println(String.format("Listener ->> created key %s, value %s",
                        event.getKey(), new String(event.getValue(), UTF_8)));
            }
        }
    }

    /**
     * Реализация методов сохранения кэша на диск.
     * <p>Работает на стороне сервера</p>
     */
    public static class ByteEntryCacheWriter implements CacheWriter<String, byte[]>, Serializable {
        public ByteEntryCacheWriter() {
            System.out.println(this.getClass().getSimpleName() + " created");
        }

        @Override
        public void write(Cache.Entry<? extends String, ? extends byte[]> entry) throws CacheWriterException {
            System.out.println(String.format("Cache writer ->> write, key: %s, value: %s",
                    entry.getKey(), new String(entry.getValue(), UTF_8)));
        }

        @Override
        public void writeAll(Collection<Cache.Entry<? extends String, ? extends byte[]>> entries) throws CacheWriterException {
            System.out.println("Cache writer ->> Write all");
        }

        @Override
        public void delete(Object key) throws CacheWriterException {
            System.out.println(String.format("Cache writer ->> delete key: %s", key));
        }

        @Override
        public void deleteAll(Collection<?> keys) throws CacheWriterException {
            System.out.println("Cache writer ->> delete all");
        }
    }

    /**
     * Реализация методов загрузки кэша с диска
     */
    public static class ByteEntryCacheLoader implements CacheLoader<String, byte[]>, Serializable {
        public ByteEntryCacheLoader() {
            System.out.println(this.getClass().getSimpleName() + " created");
        }

        @Override
        public byte[] load(String key) throws CacheLoaderException {
            return getDataFromDB(key);
        }

        @Override
        public Map<String, byte[]> loadAll(Iterable<? extends String> keys) throws CacheLoaderException {
            System.out.println("Cache loader ->> load data for all keys");
            Map<String, byte[]> ret = new HashMap<>();
            for (String key : keys) {
                ret.put(key, getDataFromDB(key));
            }
            return ret;
        }

        /**
         * Метод - имитатор загрузки данных из внешней СУБД
         *
         * @param key - ключ
         * @return данные с диска - фиксированная строка DB data for key + значение ключа
         */
        private byte[] getDataFromDB(String key) {
            System.out.println(String.format("Cache loader ->> load data for key %s", key));
            return (String.format("DB data for key %s", key)).getBytes(UTF_8);
        }
    }

    public static class SerializableSingletonFactory<T> extends SingletonFactory<T> implements Serializable {

        /**
         * Constructor for the {@link SingletonFactory}.
         *
         * @param instance the instance to return
         */
        public SerializableSingletonFactory(T instance) {
            super(instance);
        }
    }

    private static CacheManager getCacheManager() {
        // Get or create a cache manager.

        return Caching.getCachingProvider().getCacheManager(
                Paths.get("client-config.xml").toUri(), null);
    }

}
