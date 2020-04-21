package ru.mail.polis.nik27090;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;
import ru.mail.polis.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

public class DAOImpl implements DAO {

    private final SortedMap<ByteBuffer, ByteBuffer> treeMap = new TreeMap<>();

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        return treeMap.tailMap(from)
                .entrySet()
                .stream()
                .map(entry -> Record.of(entry.getKey(), entry.getValue()))
                .iterator();
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        treeMap.put(key, value);
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        treeMap.remove(key);
    }

    @Override
    public void close() throws IOException {
        treeMap.clear();
    }
}
