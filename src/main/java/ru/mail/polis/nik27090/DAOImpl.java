package ru.mail.polis.nik27090;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;
import ru.mail.polis.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.TreeMap;

public class DAOImpl implements DAO {

    private final TreeMap<ByteBuffer, Record> navigableMap = new TreeMap<>();

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull ByteBuffer from) throws IOException {
        return navigableMap.tailMap(from).values().iterator();
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException {
        navigableMap.put(key, Record.of(key, value));
    }

    @Override
    public void remove(@NotNull ByteBuffer key) throws IOException {
        navigableMap.remove(key);
    }


    @Override
    public void close() throws IOException {
        navigableMap.clear();
    }
}
