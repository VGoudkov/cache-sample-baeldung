package ru.iitdgroup.internal;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;

import java.nio.file.Paths;

public class IgniteServer {
    /**
     * Запуск сервера (как будто бы подключаемся к существующей инсталляции
     */
    public static void main(String[] args) {
        Ignite ignite = Ignition.start(Paths.get("server-config.xml").toUri().toString());
    }
}
