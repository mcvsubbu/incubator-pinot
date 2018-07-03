/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.perf;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.segment.PrefetchMode;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.core.io.reader.ReaderContext;
import com.linkedin.pinot.core.io.reader.impl.v1.FixedBitMultiValueReader;
import com.linkedin.pinot.core.io.reader.impl.v1.FixedBitSingleValueReader;
import com.linkedin.pinot.core.io.reader.impl.v1.SortedIndexReader;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.readers.DoubleDictionary;
import com.linkedin.pinot.core.segment.index.readers.FloatDictionary;
import com.linkedin.pinot.core.segment.index.readers.IntDictionary;
import com.linkedin.pinot.core.segment.index.readers.LongDictionary;
import com.linkedin.pinot.core.segment.index.readers.StringDictionary;
import com.linkedin.pinot.core.segment.store.ColumnIndexType;
import com.linkedin.pinot.core.segment.store.SegmentDirectory;
import com.linkedin.pinot.integration.tests.ClusterTest;
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;
import com.linkedin.pinot.util.TestUtils;
import java.io.File;
import java.net.URL;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;


@SuppressWarnings("unused")
@State(Scope.Benchmark)
public class BenchmarkOfflineIndexReader {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "BenchmarkOfflineIndexReader");
  private static final Random RANDOM = new Random();
  private static final URL RESOURCE_URL =
      ClusterTest.class.getClassLoader().getResource("On_Time_On_Time_Performance_2014_100k_subset_nonulls.tar.gz");
  private static final String AVRO_FILE_NAME = "On_Time_On_Time_Performance_2014_1.avro";
  private static final String TABLE_NAME = "table";

  // Forward index
  private static final String SV_UNSORTED_COLUMN_NAME = "FlightNum";
  private static final String SV_SORTED_COLUMN_NAME = "DaysSinceEpoch";
  private static final String MV_COLUMN_NAME = "DivTailNums";

  // Dictionary
  private static final int NUM_ROUNDS = 10000;
  private static final String INT_COLUMN_NAME = "DivActualElapsedTime";
  private static final String LONG_COLUMN_NAME = "DivTotalGTimes";
  private static final String FLOAT_COLUMN_NAME = "DepDelayMinutes";
  private static final String DOUBLE_COLUMN_NAME = "DepDelay";
  private static final String STRING_COLUMN_NAME = "DestCityName";

  // Forward index
  private int _numDocs;
  private FixedBitSingleValueReader _fixedBitSingleValueReader;
  private SortedIndexReader _sortedForwardIndexReader;
  private FixedBitMultiValueReader _fixedBitMultiValueReader;
  private int[] _buffer;

  // Dictionary
  private IntDictionary _intDictionary;
  private LongDictionary _longDictionary;
  private FloatDictionary _floatDictionary;
  private DoubleDictionary _doubleDictionary;
  private StringDictionary _stringDictionary;

  @Setup
  public void setUp() throws Exception {
    Preconditions.checkNotNull(RESOURCE_URL);
    FileUtils.deleteQuietly(TEMP_DIR);

    File avroDir = new File(TEMP_DIR, "avro");
    TarGzCompressionUtils.unTar(new File(TestUtils.getFileFromResourceUrl(RESOURCE_URL)), avroDir);
    File avroFile = new File(avroDir, AVRO_FILE_NAME);

    File dataDir = new File(TEMP_DIR, "index");
    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(SegmentTestUtils.getSegmentGeneratorConfigWithoutTimeColumn(avroFile, dataDir, TABLE_NAME));
    driver.build();

    File indexDir = new File(dataDir, TABLE_NAME);
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(indexDir);
    SegmentDirectory segmentDirectory = SegmentDirectory.createFromLocalFS(indexDir, segmentMetadata, ReadMode.mmap, PrefetchMode.DEFAULT_PREFETCH_MODE);
    SegmentDirectory.Reader segmentReader = segmentDirectory.createReader();

    // Forward index
    _numDocs = segmentMetadata.getTotalDocs();
    _fixedBitSingleValueReader =
        new FixedBitSingleValueReader(segmentReader.getIndexFor(SV_UNSORTED_COLUMN_NAME, ColumnIndexType.FORWARD_INDEX),
            _numDocs, segmentMetadata.getColumnMetadataFor(SV_UNSORTED_COLUMN_NAME).getBitsPerElement());
    _sortedForwardIndexReader =
        new SortedIndexReader(segmentReader.getIndexFor(SV_SORTED_COLUMN_NAME, ColumnIndexType.FORWARD_INDEX),
            segmentMetadata.getColumnMetadataFor(SV_SORTED_COLUMN_NAME).getCardinality());
    ColumnMetadata mvColumnMetadata = segmentMetadata.getColumnMetadataFor(MV_COLUMN_NAME);
    _fixedBitMultiValueReader =
        new FixedBitMultiValueReader(segmentReader.getIndexFor(MV_COLUMN_NAME, ColumnIndexType.FORWARD_INDEX), _numDocs,
            mvColumnMetadata.getTotalNumberOfEntries(), mvColumnMetadata.getBitsPerElement());
    _buffer = new int[mvColumnMetadata.getMaxNumberOfMultiValues()];

    // Dictionary
    _intDictionary = new IntDictionary(segmentReader.getIndexFor(INT_COLUMN_NAME, ColumnIndexType.DICTIONARY),
        segmentMetadata.getColumnMetadataFor(INT_COLUMN_NAME).getCardinality());
    _longDictionary = new LongDictionary(segmentReader.getIndexFor(LONG_COLUMN_NAME, ColumnIndexType.DICTIONARY),
        segmentMetadata.getColumnMetadataFor(LONG_COLUMN_NAME).getCardinality());
    _floatDictionary = new FloatDictionary(segmentReader.getIndexFor(FLOAT_COLUMN_NAME, ColumnIndexType.DICTIONARY),
        segmentMetadata.getColumnMetadataFor(FLOAT_COLUMN_NAME).getCardinality());
    _doubleDictionary = new DoubleDictionary(segmentReader.getIndexFor(DOUBLE_COLUMN_NAME, ColumnIndexType.DICTIONARY),
        segmentMetadata.getColumnMetadataFor(DOUBLE_COLUMN_NAME).getCardinality());
    ColumnMetadata stringColumnMetadata = segmentMetadata.getColumnMetadataFor(STRING_COLUMN_NAME);
    _stringDictionary = new StringDictionary(segmentReader.getIndexFor(STRING_COLUMN_NAME, ColumnIndexType.DICTIONARY),
        stringColumnMetadata.getCardinality(), stringColumnMetadata.getColumnMaxLength(), (byte) 0);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public int fixedBitSingleValueReader() {
    ReaderContext context = _fixedBitSingleValueReader.createContext();
    int ret = 0;
    for (int i = 0; i < _numDocs; i++) {
      ret += _fixedBitSingleValueReader.getInt(i, context);
    }
    return ret;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public int sortedForwardIndexReaderSequential() {
    SortedIndexReader.Context context = _sortedForwardIndexReader.createContext();
    int ret = 0;
    for (int i = 0; i < _numDocs; i++) {
      ret += _sortedForwardIndexReader.getInt(i, context);
    }
    return ret;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public int sortedForwardIndexReaderRandom() {
    SortedIndexReader.Context context = _sortedForwardIndexReader.createContext();
    int ret = 0;
    for (int i = 0; i < _numDocs; i++) {
      ret += _sortedForwardIndexReader.getInt(RANDOM.nextInt(_numDocs), context);
    }
    return ret;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public int fixedBitMultiValueReaderSequential() {
    FixedBitMultiValueReader.Context context = _fixedBitMultiValueReader.createContext();
    int ret = 0;
    for (int i = 0; i < _numDocs; i++) {
      ret += _fixedBitMultiValueReader.getIntArray(i, _buffer, context);
    }
    return ret;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public int fixedBitMultiValueReaderRandom() {
    FixedBitMultiValueReader.Context context = _fixedBitMultiValueReader.createContext();
    int ret = 0;
    for (int i = 0; i < _numDocs; i++) {
      ret += _fixedBitMultiValueReader.getIntArray(RANDOM.nextInt(_numDocs), _buffer, context);
    }
    return ret;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public double intDictionary() {
    int length = _intDictionary.length();
    int ret = 0;
    for (int i = 0; i < NUM_ROUNDS; i++) {
      int value = _intDictionary.getIntValue(RANDOM.nextInt(length));
      ret += _intDictionary.indexOf(value);
    }
    return ret;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public int longDictionary() {
    int length = _longDictionary.length();
    int ret = 0;
    for (int i = 0; i < NUM_ROUNDS; i++) {
      long value = _longDictionary.getLongValue(RANDOM.nextInt(length));
      ret += _longDictionary.indexOf(value);
    }
    return ret;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public int floatDictionary() {
    int length = _floatDictionary.length();
    int ret = 0;
    for (int i = 0; i < NUM_ROUNDS; i++) {
      float value = _floatDictionary.getFloatValue(RANDOM.nextInt(length));
      ret += _floatDictionary.indexOf(value);
    }
    return ret;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public int doubleDictionary() {
    int length = _doubleDictionary.length();
    int ret = 0;
    for (int i = 0; i < NUM_ROUNDS; i++) {
      double value = _doubleDictionary.getDoubleValue(RANDOM.nextInt(length));
      ret += _doubleDictionary.indexOf(value);
    }
    return ret;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public int stringDictionary() {
    int length = _stringDictionary.length();
    int ret = 0;
    int[] dictIds = new int[NUM_ROUNDS];
    for (int i = 0; i < NUM_ROUNDS; i++) {
      int dictId = RANDOM.nextInt(length);
      String value = _stringDictionary.getStringValue(dictId);
      ret += _stringDictionary.indexOf(value);
      dictIds[i] = dictId;
    }
    String[] outValues = new String[NUM_ROUNDS];
    _stringDictionary.readStringValues(dictIds, 0, NUM_ROUNDS, outValues, 0);
    for (int i = 0; i < NUM_ROUNDS; i++) {
      ret += outValues[0].length();
    }
    return ret;
  }

  @TearDown
  public void tearDown() {
    _fixedBitSingleValueReader.close();
    _sortedForwardIndexReader.close();
    _fixedBitMultiValueReader.close();
    _intDictionary.close();
    _longDictionary.close();
    _floatDictionary.close();
    _doubleDictionary.close();
    _stringDictionary.close();

    FileUtils.deleteQuietly(TEMP_DIR);
  }

  public static void main(String[] args) throws Exception {
    Options opt = new OptionsBuilder().include(BenchmarkOfflineIndexReader.class.getSimpleName())
        .warmupTime(TimeValue.seconds(5))
        .warmupIterations(2)
        .measurementTime(TimeValue.seconds(5))
        .measurementIterations(3)
        .forks(1)
        .build();

    new Runner(opt).run();
  }
}
