// (c) Copyright 2011 Odiago, Inc.

package org.apache.avro.file;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Implements a seekable input interface over a file read via the Hadoop FileSystem API.
 *
 * <p>In order to seek and read from a Avro container file, the input stream must
 * implement {@link org.apache.avro.file.SeekableInput}. Reading a file from the Hadoop
 * {@link org.apache.hadoop.fs.FileSystem} API gives you back an {@link
 * org.apache.hadoop.fs.FSDataInputStream}, which is not quite good enough (in particular,
 * it does not expose the total length of the stream). This class implements the required
 * SeekableInput interface using the Hadoop FileSystem API.</p>
 */
public class SeekableHadoopInput implements SeekableInput {
  /** The input stream for the file. */
  private final FSDataInputStream mInputStream;

  /** The length of the entire file stream in bytes. */
  private final long mLength;

  /**
   * Constructs a seekable input over a file, read using the Hadoop FileSystem API, which
   * allows for reading from the local filesystem or HDFS alike.
   *
   * @param conf The configuration.
   * @param path The path of the file to read from.
   * @throws IOException If there is an error.
   */
  public SeekableHadoopInput(Configuration conf, Path path) throws IOException {
    FileSystem fileSystem = path.getFileSystem(conf);

    // Open the input stream.
    mInputStream = fileSystem.open(path);

    // Figure out the length of the file.
    mLength = fileSystem.getFileStatus(path).getLen();
  }

  /** {@inheritDoc} */
  @Override
  public long length() throws IOException {
    return mLength;
  }

  /** {@inheritDoc} */
  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return mInputStream.read(b, off, len);
  }

  /** {@inheritDoc} */
  @Override
  public void seek(long p) throws IOException {
    mInputStream.seek(p);
  }

  /** {@inheritDoc} */
  @Override
  public long tell() throws IOException {
    return mInputStream.getPos();
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    mInputStream.close();
  }
}
