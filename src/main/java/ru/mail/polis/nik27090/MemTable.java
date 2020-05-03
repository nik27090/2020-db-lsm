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

    public MemTable() {
        this.sortedMap = new TreeMap<>();
        this.sizeInBytes = 0L;
        this.size = 0;
        this.sizeInBytes = 0;
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

    /*
    key = 44 + size {
        ByteBuffer - (16 + 4 + 4 + 16 + 4 + size)
        }
    value = 25 byte {
        headline - 16 byte
        tombStone - 1 byte
        timeStamp - 8 byte
        }

     result = 69 + keySize
     */
    private long sizeOfElement(final ByteBuffer key) {
        return 69L + key.limit();
    }

    private long sizeOfElement(final Value value) {
        return 74L + value.getContent().limit();
    }

    /*
        value = 74 + size {
        headline - 16 byte
        link - 4 byte
        timestamp(long) - 8 byte
        tombStone(boolean) - 1 byte
        value(ByteBuffer) - [headline - 16 byte, link - 4 byte, int - 4 byte,
                                boolean - 1 byte, byte[] - (16 + 4 + size)]
        }
        key = 44 + size{
        ByteBuffer - (16 + 4 + 4 + 16 + 4 + size)
        }

        result = 118 + keySize + valueSize
     */
    private long sizeOfElement(final ByteBuffer key, final Value value) {
        return 118L + key.limit() + value.getContent().limit();
    }

    public int size() {
        return size;
    }
}
