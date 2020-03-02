package io.heryxpc.logaggregator.command;

import io.heryxpc.logaggregator.model.Chunk;
import io.heryxpc.logaggregator.model.CommandResult;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Iterator;
import java.util.List;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class SliceCommandTest {

    private SliceCommand command;

    @Mock
    File inputFile = new File("/path");

    @Mock
    RandomAccessFile randomAccessFile;

    long maxChunkSize = 100;

    @Before
    public void setUp() {
        command = spy(new SliceCommand(inputFile, maxChunkSize));
    }

    @Test
    public void should_split_file_into_chunks() throws IOException {
        // given
        long length = 1001;
        long filePointer = 0;
        long fileLength = maxChunkSize;
        when(inputFile.length()).thenReturn(length);
        when(inputFile.exists()).thenReturn(true);
        doReturn(randomAccessFile).when(command).createRandomAccessFile(inputFile, 0L, fileLength);
        doReturn(randomAccessFile).when(command).createRandomAccessFile(inputFile, 100L, fileLength);
        doReturn(randomAccessFile).when(command).createRandomAccessFile(inputFile, 200L, fileLength);
        doReturn(randomAccessFile).when(command).createRandomAccessFile(inputFile, 300L, fileLength);
        doReturn(randomAccessFile).when(command).createRandomAccessFile(inputFile, 400L, fileLength);
        doReturn(randomAccessFile).when(command).createRandomAccessFile(inputFile, 500L, fileLength);
        doReturn(randomAccessFile).when(command).createRandomAccessFile(inputFile, 600L, fileLength);
        doReturn(randomAccessFile).when(command).createRandomAccessFile(inputFile, 700L, fileLength);
        doReturn(randomAccessFile).when(command).createRandomAccessFile(inputFile, 800L, fileLength);
        doReturn(randomAccessFile).when(command).createRandomAccessFile(inputFile, 900L, fileLength);
        doReturn(randomAccessFile).when(command).createRandomAccessFile(inputFile, 1000L, 1);
        when(randomAccessFile.getFilePointer()).thenReturn(filePointer);
        when(randomAccessFile.length()).thenReturn(filePointer + maxChunkSize + 1);
        // when
        CommandResult result = command.execute();
        // then
        Assert.assertNotNull(result.getResult());
        List<Chunk> chunks = (List<Chunk>)result.getResult();
        Assert.assertEquals(11, chunks.size());

        Iterator<Chunk> it = chunks.iterator();
        while (it.hasNext() ) {
            Chunk chunk = it.next();
            Assert.assertEquals(filePointer, chunk.getIndex());
            if ( it.hasNext() ) {
                Assert.assertEquals(maxChunkSize, chunk.getSize());
            } else {
                Assert.assertEquals(1, chunk.getSize());
            }
            RandomAccessFile raf = chunk.getOrigin();
            Assert.assertEquals(randomAccessFile, raf);
            filePointer += maxChunkSize;
        }
    }

    @Test
    public void should_do_nothing_if_file_not_found() {
        // given
        when(inputFile.getPath()).thenReturn("/filepath");
        when(inputFile.exists()).thenReturn(false);
        // when
        CommandResult result = command.execute();
        // then
        Assert.assertNull(result.getResult());
    }

}
