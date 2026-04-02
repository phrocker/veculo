/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.file.rfile;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;

/**
 * Vector index metadata for RFile blocks containing vector data. This enables efficient vector
 * similarity searches by storing centroids and other metadata for coarse filtering.
 */
public class VectorIndex implements Writable {

  /**
   * Metadata for a single vector block.
   */
  public static class VectorBlockMetadata implements Writable {
    private float[] centroid;
    private int vectorCount;
    private long blockOffset;
    private int blockSize;
    private byte[] visibility; // Visibility markings for this block
    private boolean compressed; // Whether vectors in this block are compressed
    private byte compressionType; // Type of compression used (0=none, 1=quantized8, 2=quantized16)

    public VectorBlockMetadata() {
      // Default constructor for Writable
      this.visibility = new byte[0];
      this.compressed = false;
      this.compressionType = 0;
    }

    public VectorBlockMetadata(float[] centroid, int vectorCount, long blockOffset, int blockSize) {
      this.centroid = centroid;
      this.vectorCount = vectorCount;
      this.blockOffset = blockOffset;
      this.blockSize = blockSize;
      this.visibility = new byte[0];
      this.compressed = false;
      this.compressionType = 0;
    }

    public VectorBlockMetadata(float[] centroid, int vectorCount, long blockOffset, int blockSize,
        byte[] visibility, boolean compressed, byte compressionType) {
      this.centroid = centroid;
      this.vectorCount = vectorCount;
      this.blockOffset = blockOffset;
      this.blockSize = blockSize;
      this.visibility = visibility != null ? visibility : new byte[0];
      this.compressed = compressed;
      this.compressionType = compressionType;
    }

    public float[] getCentroid() {
      return centroid;
    }

    public int getVectorCount() {
      return vectorCount;
    }

    public long getBlockOffset() {
      return blockOffset;
    }

    public int getBlockSize() {
      return blockSize;
    }

    public byte[] getVisibility() {
      return visibility;
    }

    public boolean isCompressed() {
      return compressed;
    }

    public byte getCompressionType() {
      return compressionType;
    }

    public void setVisibility(byte[] visibility) {
      this.visibility = visibility != null ? visibility : new byte[0];
    }

    public void setCompressed(boolean compressed) {
      this.compressed = compressed;
    }

    public void setCompressionType(byte compressionType) {
      this.compressionType = compressionType;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(centroid.length);
      for (float value : centroid) {
        out.writeFloat(value);
      }
      out.writeInt(vectorCount);
      out.writeLong(blockOffset);
      out.writeInt(blockSize);

      // Write visibility data
      out.writeInt(visibility.length);
      if (visibility.length > 0) {
        out.write(visibility);
      }

      // Write compression metadata
      out.writeBoolean(compressed);
      out.writeByte(compressionType);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      int dimension = in.readInt();
      centroid = new float[dimension];
      for (int i = 0; i < dimension; i++) {
        centroid[i] = in.readFloat();
      }
      vectorCount = in.readInt();
      blockOffset = in.readLong();
      blockSize = in.readInt();

      // Read visibility data
      int visibilityLength = in.readInt();
      visibility = new byte[visibilityLength];
      if (visibilityLength > 0) {
        in.readFully(visibility);
      }

      // Read compression metadata
      compressed = in.readBoolean();
      compressionType = in.readByte();
    }
  }

  private int vectorDimension;
  private List<VectorBlockMetadata> blocks;

  public VectorIndex() {
    this.blocks = new ArrayList<>();
  }

  public VectorIndex(int vectorDimension) {
    this.vectorDimension = vectorDimension;
    this.blocks = new ArrayList<>();
  }

  public void addBlock(VectorBlockMetadata block) {
    blocks.add(block);
  }

  public List<VectorBlockMetadata> getBlocks() {
    return blocks;
  }

  public int getVectorDimension() {
    return vectorDimension;
  }

  public void setVectorDimension(int vectorDimension) {
    this.vectorDimension = vectorDimension;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(vectorDimension);
    out.writeInt(blocks.size());
    for (VectorBlockMetadata block : blocks) {
      block.write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    vectorDimension = in.readInt();
    int blockCount = in.readInt();
    blocks = new ArrayList<>(blockCount);
    for (int i = 0; i < blockCount; i++) {
      VectorBlockMetadata block = new VectorBlockMetadata();
      block.readFields(in);
      blocks.add(block);
    }
  }
}
