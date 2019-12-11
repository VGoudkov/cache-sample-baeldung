package ru.iitdgroup.internal;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.spi.CachingProvider;
import java.nio.charset.StandardCharsets;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        CachingProvider cachingProvider = Caching.getCachingProvider();
        CacheManager cacheManager = cachingProvider.getCacheManager();
        MutableConfiguration<String, byte[]> config
                = new MutableConfiguration<>();
        Cache<String, byte[]> cache = cacheManager
                .createCache("simpleCache", config);
        cache.put("key1", "value1".getBytes(StandardCharsets.UTF_8));
        cache.put("key2", "value2".getBytes(StandardCharsets.UTF_8));

        cache.forEach(entry -> System.out.println(entry.getKey()+" : "+new String(entry.getValue())));

        cachingProvider.close();
        System.out.println("Should stop here");
    }
}
