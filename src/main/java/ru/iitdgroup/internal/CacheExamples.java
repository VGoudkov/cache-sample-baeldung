package ru.iitdgroup.internal;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.*;
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
 *
 * <p>Ожидаемый вывод при первом запуске (на пустых кэшах)</p>
 * <li>Listener ->> created</li>
 * <li>Listener ->> created</li>
 * <li>------------- listing cache -----------------</li>
 * <li>Listener ->> created</li>
 * <li>key1 : value1</li>
 * <li>key2 : value2</li>
 * <li>key3 : New3</li>
 * </p>
 * <p></p>
 * <p>Ожидаемый вывод при втором запуске (на заполненных кэшах)</p>
 * <p>
 * <li>Listener ->> Updated: key = key1, value = value1</li>
 * <li>Listener ->> Updated: key = key2, value = value2</li>
 * <li>------------- listing cache -----------------</li>
 * <li>key1 : value1</li>
 * <li>key2 : value2</li>
 * <li>key3 : New3</li>
 * </p>
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


            config
                    .setWriteThrough(true)
                    .setCacheWriterFactory(FactoryBuilder.factoryOf(ByteEntryCacheWriter.class))
                    .setReadThrough(true)
                    .setCacheLoaderFactory(FactoryBuilder.factoryOf(ByteEntryCacheLoader.class))
            ;


            Cache<String, byte[]> cache;

            cache = cacheManager.getCache(CACHE_NAME);

            if (cache == null) {
                cache = cacheManager.createCache(CACHE_NAME, config);
            }


            registerListener(cache);

            final String nonExustentKey = "NON - Exstent key";
            final byte[] data = cache.get(nonExustentKey);
            System.out.println(String.format("Getting non-existent cache entry: key %s, value: %s",
                    nonExustentKey,
                    new String(data==null?"NULL".getBytes(UTF_8):data, UTF_8)));

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
     *
     * @param cache кэш, для которого регистрировать
     */
    private static void registerListener(Cache<String, byte[]> cache) {
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

    /**
     * Класс для обработки записей на стороне сервера кэша.
     * <p>Сериализуется и уезжает работать на стороне сервера!</p>
     */
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

    /**
     * Класс для обработки событий изменения записей в кэше.
     * <p>Работает ТОЛЬКО на get/put/... и НЕ вызывается при обработке с помощью Entry processor</p>
     */
    public static class ByteEntryUpdateListener implements CacheEntryUpdatedListener<String, byte[]>, CacheEntryCreatedListener<String, byte[]> {
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
                System.out.println("Listener ->> created");
            }
        }
    }

    /**
     * Вспомогательная фабрика для создания класса с методами подписки на события кэша.
     * FIXME: возможно, что-то сделано не так с точки зрения логики javax.cache и этого вообше не должно быть
     */
    public static class LoggingEntryListenerFactory implements Factory<ByteEntryUpdateListener> {
        @Override
        public ByteEntryUpdateListener create() {
            return new ByteEntryUpdateListener();
        }
    }


    /**
     * Реализация методов сохранения кэша на диск.
     * <p>Работает на стороне сервера</p>
     */
    public static class ByteEntryCacheWriter implements CacheWriter<String, byte[]> {
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
    public static class ByteEntryCacheLoader implements CacheLoader<String, byte[]> {
        @Override
        public byte[] load(String key) throws CacheLoaderException {
            return getDataFromDB(key);
        }

        public ByteEntryCacheLoader() {
            System.out.println( this.getClass().getSimpleName()+" created");
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

}
