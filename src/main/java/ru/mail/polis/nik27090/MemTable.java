package ru.mail.polis.nik27090;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

public class MemTable implements Table {
    private final SortedMap<ByteBuffer, Value> sortedMap;
    private long sizeInBytes;
    private int size;

    /**
     * Creates table of data in memory.
     */
    public MemTable() {
        this.sortedMap = new TreeMap<>();
        this.sizeInBytes = 0L;
        this.size = 0;
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) {
        return sortedMap.tailMap(from)
                .entrySet()
                .stream()
                .map(entry -> new Cell(entry.getKey(), entry.getValue()))
                .iterator();
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        final Value valueOfElement = new Value(System.currentTimeMillis(), value.duplicate());
        final Value prevValue = sortedMap.put(key.duplicate(), valueOfElement);
        if (prevValue == null) {
            sizeInBytes += sizeOfElement(key, valueOfElement);
            size++;
        } else if (prevValue.isTombStone()) {
            sizeInBytes += sizeOfElement(valueOfElement);
        } else {
            sizeInBytes += sizeOfElement(valueOfElement) - sizeOfElement(prevValue.getContent());
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        final Value valueOfElement = new Value(System.currentTimeMillis(), true);
        final Value prevValue = sortedMap.put(key.duplicate(), valueOfElement);
        if (prevValue == null) {
            sizeInBytes += sizeOfElement(key);
            size++;
        }
    }

    public long getSizeInBytes() {
        return sizeInBytes;
    }

    /**
     * approximate Cell size calculation with dead value.
     * key = 44 + size = (ByteBuffer - (16 + 4 + 4 + 16 + 4 + size))
     * +
     * value = 25 = headline - 16, tombStone - 1, timeStamp - 8
     * =
     * 69 + keySize
     *
     * @param key key of Cell.
     */
    private long sizeOfElement(final ByteBuffer key) {
        return 69L + key.limit();
    }

    /**
     * approximate Value of Cell size calculation.
     *
     * @param value value of Cell
     */
    private long sizeOfElement(final Value value) {
        return 74L + value.getContent().limit();
    }

    /**
     * approximate Cell size calculation with alive value.
     * value = 74 + size (headline - 16, link - 4, timestamp(long) - 8, tombStone(boolean) - 1,
     * content(ByteBuffer) - (headline - 16, link - 4, int - 4, boolean - 1, byte[] - (16 + 4 + size)))
     * +
     * key = 44 + size (ByteBuffer - (16 + 4 + 4 + 16 + 4 + size))
     * =
     * 118 + keySize + contentSize
     *
     * @param key   key of Cell
     * @param value value of Cell
     */
    private long sizeOfElement(final ByteBuffer key, final Value value) {
        return 118L + key.limit() + value.getContent().limit();
    }

    public int size() {
        return size;
    }
}
