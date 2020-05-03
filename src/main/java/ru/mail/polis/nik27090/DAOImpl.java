package ru.mail.polis.nik27090;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Client;
import ru.mail.polis.DAO;
import ru.mail.polis.Iters;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.stream.Stream;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;

public class DAOImpl implements DAO {
//    private static final Logger log = LoggerFactory.getLogger(Client.class);

    private static final String SUFFIX = ".dat";
    private static final String TEMP = ".tmp";

    private MemTable memTable;
    private final NavigableMap<Integer, SSTable> ssTables;

    @NotNull
    private final File storage;
    private final long flushSize;
    private final static double coeff = 0.005;

    private int generation;

    public DAOImpl(
            @NotNull final File storage,
            final long heapSize) throws IOException {
        this.storage = storage;
        this.flushSize = (long) (heapSize * coeff);
        assert flushSize > 0L;
        this.ssTables = new TreeMap<>();
        this.memTable = new MemTable();
        this.generation = -1;

        try (Stream<Path> paths = Files.list(storage.toPath())) {
            paths
                    .filter(path -> path.toString().endsWith(SUFFIX))
                    .forEach(element -> {
                        try {
                            final String name = element.getFileName().toString();
                            final int generation = Integer.parseInt(name.substring(0, name.indexOf(SUFFIX)));
                            this.generation = Math.max(this.generation, generation);
                            ssTables.put(generation, new SSTable(element.toFile()));
                        } catch (Exception e) {
                            //todo log
                        }
                    });
        }
        generation++;
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        final List<Iterator<Cell>> iterators = new ArrayList<>(ssTables.size() + 1);
        iterators.add(memTable.iterator(from));
        ssTables.descendingMap().values().forEach(ssTable -> {
                iterators.add(ssTable.iterator(from));
        });
        // sorted duplicates
        final Iterator<Cell> merged = Iterators.mergeSorted(iterators, Cell.COMPARATOR);
        //one cell per key
        final Iterator<Cell> fresh = Iters.collapseEquals(merged, Cell::getKey);
        final Iterator<Cell> alive = Iterators.filter(fresh, el -> !el.getValue().isTombStone());
        return Iterators.transform(alive, el -> Record.of(el.getKey(), el.getValue().getContent()));
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        memTable.upsert(key, value);
        if (memTable.getSizeInBytes() > flushSize) {
            flush();
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        memTable.remove(key);
        if (memTable.getSizeInBytes() > flushSize) {
            flush();
        }
    }

    private void flush() throws IOException {
        final File file = new File(storage, generation + TEMP);
        SSTable.serialize(
                file,
                memTable.iterator(ByteBuffer.allocate(0)),
                memTable.size());
        final File dst = new File(storage, generation + SUFFIX);
        Files.move(file.toPath(), dst.toPath(), StandardCopyOption.ATOMIC_MOVE);

        memTable = new MemTable();
        ssTables.put(generation, new SSTable(dst));
        generation++;
    }

    @Override
    public void close() throws IOException {
        flush();
        ssTables.values().forEach(SSTable::closeChannel);
    }
}
