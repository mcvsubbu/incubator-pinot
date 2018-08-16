/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.segment.creator.impl;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.config.ColumnPartitionConfig;
import com.linkedin.pinot.common.data.DateTimeFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.StarTreeIndexSpec;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.partition.PartitionFunction;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.indexsegment.mutable.MutableSegmentImpl;
import com.linkedin.pinot.core.io.compression.ChunkCompressorFactory;
import com.linkedin.pinot.core.io.reader.SingleColumnSingleValueReader;
import com.linkedin.pinot.core.io.readerwriter.impl.FixedByteSingleColumnSingleValueReaderWriter;
import com.linkedin.pinot.core.realtime.converter.stats.RealtimeSegmentSegmentCreationDataSource;
import com.linkedin.pinot.core.realtime.impl.dictionary.BaseOffHeapMutableDictionary;
import com.linkedin.pinot.core.segment.creator.ColumnIndexCreationInfo;
import com.linkedin.pinot.core.segment.creator.ForwardIndexCreator;
import com.linkedin.pinot.core.segment.creator.InvertedIndexCreator;
import com.linkedin.pinot.core.segment.creator.MultiValueForwardIndexCreator;
import com.linkedin.pinot.core.segment.creator.SegmentCreationDataSource;
import com.linkedin.pinot.core.segment.creator.SegmentCreator;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationInfo;
import com.linkedin.pinot.core.segment.creator.SingleValueForwardIndexCreator;
import com.linkedin.pinot.core.segment.creator.SingleValueRawIndexCreator;
import com.linkedin.pinot.core.segment.creator.impl.fwd.MultiValueUnsortedForwardIndexCreator;
import com.linkedin.pinot.core.segment.creator.impl.fwd.SingleValueFixedByteRawIndexCreator;
import com.linkedin.pinot.core.segment.creator.impl.fwd.SingleValueSortedForwardIndexCreator;
import com.linkedin.pinot.core.segment.creator.impl.fwd.SingleValueUnsortedForwardIndexCreator;
import com.linkedin.pinot.core.segment.creator.impl.fwd.SingleValueVarByteRawIndexCreator;
import com.linkedin.pinot.core.segment.creator.impl.inv.OffHeapBitmapInvertedIndexCreator;
import com.linkedin.pinot.core.segment.creator.impl.inv.OnHeapBitmapInvertedIndexCreator;
import com.linkedin.pinot.core.segment.index.data.source.ColumnDataSource;
import com.linkedin.pinot.startree.hll.HllConfig;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.math.IntRange;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.pinot.core.segment.creator.impl.V1Constants.MetadataKeys.Column.*;
import static com.linkedin.pinot.core.segment.creator.impl.V1Constants.MetadataKeys.Segment.*;
import static com.linkedin.pinot.core.segment.creator.impl.V1Constants.MetadataKeys.StarTree.*;


/**
 * Segment creator which writes data in a columnar form.
 */
// TODO: check resource leaks
public class SegmentColumnarIndexCreator implements SegmentCreator {
  // TODO Refactor class name to match interface name
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentColumnarIndexCreator.class);
  private SegmentGeneratorConfig config;
  private Map<String, ColumnIndexCreationInfo> indexCreationInfoMap;
  private Map<String, SegmentDictionaryCreator> _dictionaryCreatorMap = new HashMap<>();
  private Map<String, Integer> _numDictionaryBytesPerEntry = new HashMap<>();
  private Map<String, Integer> _dictionarySize = new HashMap<>();
  private Map<String, ForwardIndexCreator> _forwardIndexCreatorMap = new HashMap<>();
  private Map<String, InvertedIndexCreator> _invertedIndexCreatorMap = new HashMap<>();
  private String segmentName;
  private Schema schema;
  private File _indexDir;
  private int totalDocs;
  private int totalRawDocs;
  private int totalAggDocs;
  private int totalErrors;
  private int totalNulls;
  private int totalConversions;
  private int totalNullCols;
  private int docIdCounter;
  private final MutableSegmentImpl _mutableSegment;
  private final int[] _newToOldDocId;
  private final int[] _oldToNewDocId;

  public SegmentColumnarIndexCreator(SegmentCreationDataSource dataSource) {
    if (dataSource instanceof RealtimeSegmentSegmentCreationDataSource) {
      _mutableSegment = ((RealtimeSegmentSegmentCreationDataSource)dataSource).getRealtimeSegment();
      _newToOldDocId = ((RealtimeSegmentSegmentCreationDataSource)dataSource).getSortedDocIdIterator();
      if (_newToOldDocId != null) {
        _oldToNewDocId = new int[_newToOldDocId.length];
        for (int i = 0; i < _newToOldDocId.length; i++) {
          _oldToNewDocId[_newToOldDocId[i]] = i;
        }
      } else {
        _oldToNewDocId = null;
      }
    } else {
      _mutableSegment = null;
      _newToOldDocId = null;
      _oldToNewDocId = null;
    }
  }

  @Override
  public void init(SegmentGeneratorConfig segmentCreationSpec, SegmentIndexCreationInfo segmentIndexCreationInfo,
      Map<String, ColumnIndexCreationInfo> indexCreationInfoMap, Schema schema, File outDir) throws Exception {
    docIdCounter = 0;
    config = segmentCreationSpec;
    this.indexCreationInfoMap = indexCreationInfoMap;

    // Check that the output directory does not exist
    Preconditions.checkState(!outDir.exists(), "Segment output directory: %s already exists", outDir);

    Preconditions.checkState(outDir.mkdirs(), "Failed to create output directory: %s", outDir);
    _indexDir = outDir;

    this.schema = schema;
    this.totalDocs = segmentIndexCreationInfo.getTotalDocs();
    this.totalAggDocs = segmentIndexCreationInfo.getTotalAggDocs();
    this.totalRawDocs = segmentIndexCreationInfo.getTotalRawDocs();
    this.totalErrors = segmentIndexCreationInfo.getTotalErrors();
    this.totalNulls = segmentIndexCreationInfo.getTotalNulls();
    this.totalConversions = segmentIndexCreationInfo.getTotalConversions();
    this.totalNullCols = segmentIndexCreationInfo.getTotalNullCols();

    Collection<FieldSpec> fieldSpecs = schema.getAllFieldSpecs();
    Set<String> invertedIndexColumns = new HashSet<>();
    for (String columnName : config.getInvertedIndexCreationColumns()) {
      Preconditions.checkState(schema.hasColumn(columnName),
          "Cannot create inverted index for column: %s because it is not in schema", columnName);
      invertedIndexColumns.add(columnName);
    }

    // Initialize creators for dictionary, forward index and inverted index
    for (FieldSpec fieldSpec : fieldSpecs) {
      String columnName = fieldSpec.getName();
      ColumnIndexCreationInfo indexCreationInfo = indexCreationInfoMap.get(columnName);
      Preconditions.checkNotNull(indexCreationInfo, "Missing index creation info for column: %s", columnName);

      if (createDictionaryForColumn(indexCreationInfo, segmentCreationSpec, fieldSpec)) {
        // Create dictionary-encoded index

        // TODO: hasNulls is always false, for null value we replace it with default null value
        boolean hasNulls = indexCreationInfo.hasNulls();

        // Initialize dictionary creator
        SegmentDictionaryCreator dictionaryCreator = new SegmentDictionaryCreator(fieldSpec, _indexDir, _mutableSegment);

//        _dictionaryCreatorMap.put(columnName, dictionaryCreator);
        // Initialize inverted index creator
        final int cardinality = dictionaryCreator.getCardinality();
        _dictionarySize.put(columnName, cardinality);

        InvertedIndexCreator invertedIndexCreator = null;
        // Initialize inverted index creator
        if (invertedIndexColumns.contains(columnName)) {
          if (segmentCreationSpec.isOnHeap()) {
            invertedIndexCreator =
                new OnHeapBitmapInvertedIndexCreator(_indexDir, columnName, cardinality);
          } else {
            invertedIndexCreator =
                new OffHeapBitmapInvertedIndexCreator(_indexDir, fieldSpec, cardinality, totalDocs,
                    indexCreationInfo.getTotalNumberOfEntries());
          }
        }
        // Create dictionary
        try {
            dictionaryCreator.build();
            _numDictionaryBytesPerEntry.put(columnName, dictionaryCreator.getNumBytesPerEntry());
        } catch (Exception e) {
          LOGGER.error("Error building dictionary for field: {}, cardinality: {}, number of bytes per entry: {}",
              fieldSpec.getName(), indexCreationInfo.getDistinctValueCount(), dictionaryCreator.getNumBytesPerEntry());
          throw e;
        }

        ForwardIndexCreator forwardIndexCreator;
        // Initialize forward index creator
        if (fieldSpec.isSingleValueField()) {
          if (indexCreationInfo.isSorted()) {
            forwardIndexCreator =
                new SingleValueSortedForwardIndexCreator(_indexDir, cardinality, fieldSpec, dictionaryCreator, _mutableSegment.getSVFwdIndex(columnName), totalDocs, _newToOldDocId);
          } else {
            forwardIndexCreator =
                new SingleValueUnsortedForwardIndexCreator(fieldSpec, _indexDir, cardinality, totalDocs, totalDocs,
                    hasNulls, dictionaryCreator, _mutableSegment.getSVFwdIndex(columnName), _newToOldDocId);
          }
        } else {
          forwardIndexCreator =
              new MultiValueUnsortedForwardIndexCreator(fieldSpec, _indexDir, cardinality, totalDocs,
                  indexCreationInfo.getTotalNumberOfEntries(), hasNulls, dictionaryCreator, _mutableSegment.getMVFwdIndex(columnName),
                  _newToOldDocId);
        }
        forwardIndexCreator.build(invertedIndexCreator);
        forwardIndexCreator.close();
        dictionaryCreator.close();
        if (invertedIndexCreator != null) {
          invertedIndexCreator.seal();
          invertedIndexCreator.close();
        }

      } else {
        // Create raw index

        // TODO: add support to multi-value column and inverted index
        Preconditions.checkState(fieldSpec.isSingleValueField(), "Cannot create raw index for multi-value column: %s",
            columnName);
        Preconditions.checkState(!invertedIndexColumns.contains(columnName),
            "Cannot create inverted index for raw index column: %s", columnName);

        ChunkCompressorFactory.CompressionType compressionType =
            getColumnCompressionType(segmentCreationSpec, fieldSpec);

        // Initialize forward index creator
        ForwardIndexCreator forwardIndexCreator =
            getRawIndexCreatorForColumn(_indexDir, compressionType, columnName, fieldSpec.getDataType(), totalDocs,
                indexCreationInfo.getLengthOfLongestEntry(), _mutableSegment);
          // Should be non-null for realtime data creation
        ColumnDataSource dataSource = _mutableSegment.getDataSource(columnName);
        if (dataSource.getDataSourceMetadata().hasDictionary()) {
          BaseOffHeapMutableDictionary mutableDictionary = (BaseOffHeapMutableDictionary) dataSource.getDictionary();
          FixedByteSingleColumnSingleValueReaderWriter fwdIndex = _mutableSegment.getSVFwdIndex(columnName);
          for (int newDocId = 0; newDocId < totalDocs; newDocId++) {
            int oldDocId = _newToOldDocId[newDocId];
            int oldDictId = fwdIndex.getInt(oldDocId);
            Object value = mutableDictionary.get(oldDictId);
            ((SingleValueRawIndexCreator) forwardIndexCreator).index(newDocId, value);
          }
        } else {
          forwardIndexCreator.build(null);
        }
        forwardIndexCreator.close();
      }
    }
  }

  /**
   * Helper method that returns compression type to use based on segment creation spec and field type.
   * <ul>
   *   <li> Returns compression type from segment creation spec, if specified there.</li>
   *   <li> Else, returns PASS_THROUGH for metrics, and SNAPPY for dimensions. This is because metrics are likely
   *        to be spread in different chunks after applying predicates. Same could be true for dimensions, but in that
   *        case, clients are expected to explicitly specify the appropriate compression type in the spec. </li>
   * </ul>
   * @param segmentCreationSpec Segment creation spec
   * @param fieldSpec Field spec for the column
   * @return Compression type to use
   */
  private ChunkCompressorFactory.CompressionType getColumnCompressionType(SegmentGeneratorConfig segmentCreationSpec,
      FieldSpec fieldSpec) {
    ChunkCompressorFactory.CompressionType compressionType =
        segmentCreationSpec.getRawIndexCompressionType().get(fieldSpec.getName());

    if (compressionType == null) {
      if (fieldSpec.getFieldType().equals(FieldType.METRIC)) {
        return ChunkCompressorFactory.CompressionType.PASS_THROUGH;
      } else {
        return ChunkCompressorFactory.CompressionType.SNAPPY;
      }
    } else {
      return compressionType;
    }
  }

  /**
   * Returns true if dictionary should be created for a column, false otherwise.
   * Currently there are two sources for this config:
   * <ul>
   *   <li> ColumnIndexCreationInfo (this is currently hard-coded to always return dictionary). </li>
   *   <li> SegmentGeneratorConfig</li>
   * </ul>
   *
   * This method gives preference to the SegmentGeneratorConfig first.
   *
   * @param info Column index creation info
   * @param config Segment generation config
   * @param spec Field spec for the column
   * @return True if dictionary should be created for the column, false otherwise
   */
  private boolean createDictionaryForColumn(ColumnIndexCreationInfo info, SegmentGeneratorConfig config,
      FieldSpec spec) {
    String column = spec.getName();

    if (config.getRawIndexCreationColumns().contains(column) || config.getRawIndexCompressionType()
        .containsKey(column)) {
      if (!spec.isSingleValueField()) {
        throw new RuntimeException(
            "Creation of indices without dictionaries is supported for single valued columns only.");
      }
      return false;
    } else if (spec.getDataType().equals(FieldSpec.DataType.BYTES) && !info.isFixedLength()) {
      return false;
    }
    return info.isCreateDictionary();
  }

  @Override
  public void indexRow(GenericRow row) {
    for (String columnName : _forwardIndexCreatorMap.keySet()) {
      Object columnValueToIndex = row.getValue(columnName);
      if (columnValueToIndex == null) {
        throw new RuntimeException("Null value for column:" + columnName);
      }

      SegmentDictionaryCreator dictionaryCreator = _dictionaryCreatorMap.get(columnName);
      if (schema.getFieldSpecFor(columnName).isSingleValueField()) {
        if (dictionaryCreator != null) {
          int dictId = dictionaryCreator.indexOfSV(columnValueToIndex);
          ((SingleValueForwardIndexCreator) _forwardIndexCreatorMap.get(columnName)).index(docIdCounter, dictId);
          if (_invertedIndexCreatorMap.containsKey(columnName)) {
            _invertedIndexCreatorMap.get(columnName).add(dictId);
          }
        } else {
          ((SingleValueRawIndexCreator) _forwardIndexCreatorMap.get(columnName)).index(docIdCounter,
              columnValueToIndex);
        }
      } else {
        int[] dictIds = dictionaryCreator.indexOfMV(columnValueToIndex);
        ((MultiValueForwardIndexCreator) _forwardIndexCreatorMap.get(columnName)).index(docIdCounter, dictIds);
        if (_invertedIndexCreatorMap.containsKey(columnName)) {
          _invertedIndexCreatorMap.get(columnName).add(dictIds, dictIds.length);
        }
      }
    }
    docIdCounter++;
  }

  @Override
  public void setSegmentName(String segmentName) {
    this.segmentName = segmentName;
  }

  @Override
  public void seal() throws ConfigurationException, IOException {
    for (InvertedIndexCreator invertedIndexCreator : _invertedIndexCreatorMap.values()) {
      invertedIndexCreator.seal();
    }
    writeMetadata();
  }

  void writeMetadata() throws ConfigurationException {
    PropertiesConfiguration properties =
        new PropertiesConfiguration(new File(_indexDir, V1Constants.MetadataKeys.METADATA_FILE_NAME));

    properties.setProperty(SEGMENT_CREATOR_VERSION, config.getCreatorVersion());
    properties.setProperty(SEGMENT_PADDING_CHARACTER, String.valueOf(V1Constants.Str.DEFAULT_STRING_PAD_CHAR));
    properties.setProperty(SEGMENT_NAME, segmentName);
    properties.setProperty(TABLE_NAME, config.getTableName());
    properties.setProperty(DIMENSIONS, config.getDimensions());
    properties.setProperty(METRICS, config.getMetrics());
    properties.setProperty(DATETIME_COLUMNS, config.getDateTimeColumnNames());
    properties.setProperty(TIME_COLUMN_NAME, config.getTimeColumnName());
    properties.setProperty(TIME_INTERVAL, "not_there");
    properties.setProperty(SEGMENT_TOTAL_RAW_DOCS, String.valueOf(totalRawDocs));
    properties.setProperty(SEGMENT_TOTAL_AGGREGATE_DOCS, String.valueOf(totalAggDocs));
    properties.setProperty(SEGMENT_TOTAL_DOCS, String.valueOf(totalDocs));
    properties.setProperty(STAR_TREE_ENABLED, String.valueOf(config.isEnableStarTreeIndex()));
    properties.setProperty(SEGMENT_TOTAL_ERRORS, String.valueOf(totalErrors));
    properties.setProperty(SEGMENT_TOTAL_NULLS, String.valueOf(totalNulls));
    properties.setProperty(SEGMENT_TOTAL_CONVERSIONS, String.valueOf(totalConversions));
    properties.setProperty(SEGMENT_TOTAL_NULL_COLS, String.valueOf(totalNullCols));

    StarTreeIndexSpec starTreeIndexSpec = config.getStarTreeIndexSpec();
    if (starTreeIndexSpec != null) {
      properties.setProperty(STAR_TREE_SPLIT_ORDER, starTreeIndexSpec.getDimensionsSplitOrder());
      properties.setProperty(STAR_TREE_MAX_LEAF_RECORDS, starTreeIndexSpec.getMaxLeafRecords());
      properties.setProperty(STAR_TREE_SKIP_STAR_NODE_CREATION_FOR_DIMENSIONS,
          starTreeIndexSpec.getSkipStarNodeCreationForDimensions());
      properties.setProperty(STAR_TREE_SKIP_MATERIALIZATION_CARDINALITY,
          starTreeIndexSpec.getSkipMaterializationCardinalityThreshold());
      properties.setProperty(STAR_TREE_SKIP_MATERIALIZATION_FOR_DIMENSIONS,
          starTreeIndexSpec.getSkipMaterializationForDimensions());
    }

    HllConfig hllConfig = config.getHllConfig();
    Map<String, String> derivedHllFieldToOriginMap = null;
    if (hllConfig != null) {
      properties.setProperty(SEGMENT_HLL_LOG2M, hllConfig.getHllLog2m());
      derivedHllFieldToOriginMap = hllConfig.getDerivedHllFieldToOriginMap();
    }

    // Write time related metadata (start time, end time, time unit)
    String timeColumn = config.getTimeColumnName();
    ColumnIndexCreationInfo timeColumnIndexCreationInfo = indexCreationInfoMap.get(timeColumn);
    if (timeColumnIndexCreationInfo != null) {
      // Use start/end time in config if defined
      if (config.getStartTime() != null && config.getEndTime() != null) {
        properties.setProperty(SEGMENT_START_TIME, config.getStartTime());
        properties.setProperty(SEGMENT_END_TIME, config.getEndTime());
      } else {
        Object minTime = timeColumnIndexCreationInfo.getMin();
        Object maxTime = timeColumnIndexCreationInfo.getMax();

        // Convert time value into millis since epoch for SIMPLE_DATE
        if (config.getTimeColumnType() == SegmentGeneratorConfig.TimeColumnType.SIMPLE_DATE) {
          DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern(config.getSimpleDateFormat());
          properties.setProperty(SEGMENT_START_TIME, dateTimeFormatter.parseMillis(minTime.toString()));
          properties.setProperty(SEGMENT_END_TIME, dateTimeFormatter.parseMillis(maxTime.toString()));
        } else {
          properties.setProperty(SEGMENT_START_TIME, minTime);
          properties.setProperty(SEGMENT_END_TIME, maxTime);
        }
      }

      properties.setProperty(TIME_UNIT, config.getSegmentTimeUnit());
    }

    for (Map.Entry<String, String> entry : config.getCustomProperties().entrySet()) {
      properties.setProperty(entry.getKey(), entry.getValue());
    }

    for (Map.Entry<String, ColumnIndexCreationInfo> entry : indexCreationInfoMap.entrySet()) {
      String column = entry.getKey();
      ColumnIndexCreationInfo columnIndexCreationInfo = entry.getValue();
      Integer dictionaryElementSize = _numDictionaryBytesPerEntry.get(column) == null ? 0 : _numDictionaryBytesPerEntry.get(column);
      Integer distinctValueCount = _dictionarySize.get(column) == null ? Integer.MIN_VALUE : _dictionarySize.get(column);

      // TODO: after fixing the server-side dependency on HAS_INVERTED_INDEX and deployed, set HAS_INVERTED_INDEX properly
      // The hasInvertedIndex flag in segment metadata is picked up in ColumnMetadata, and will be used during the query
      // plan phase. If it is set to false, then inverted indexes are not used in queries even if they are created via table
      // configs on segment load. So, we set it to true here for now, until we fix the server to update the value inside
      // ColumnMetadata, export information to the query planner that the inverted index available is current and can be used.
      //
      //    boolean hasInvertedIndex = invertedIndexCreatorMap.containsKey();
      boolean hasInvertedIndex = true;

      String hllOriginColumn = null;
      if (derivedHllFieldToOriginMap != null) {
        hllOriginColumn = derivedHllFieldToOriginMap.get(column);
      }

      addColumnMetadataInfo(properties, column, columnIndexCreationInfo, totalDocs, totalRawDocs, totalAggDocs,
          schema.getFieldSpecFor(column), _numDictionaryBytesPerEntry.containsKey(column), dictionaryElementSize,
          hasInvertedIndex, hllOriginColumn, distinctValueCount);
    }

    properties.save();
  }

  public static void addColumnMetadataInfo(PropertiesConfiguration properties, String column,
      ColumnIndexCreationInfo columnIndexCreationInfo, int totalDocs, int totalRawDocs, int totalAggDocs,
      FieldSpec fieldSpec, boolean hasDictionary, Integer dictionaryElementSize, boolean hasInvertedIndex,
      String hllOriginColumn, Integer distinctValueCount) {
    properties.setProperty(getKeyFor(column, CARDINALITY), String.valueOf(distinctValueCount));
    properties.setProperty(getKeyFor(column, TOTAL_DOCS), String.valueOf(totalDocs));
    properties.setProperty(getKeyFor(column, TOTAL_RAW_DOCS), String.valueOf(totalRawDocs));
    properties.setProperty(getKeyFor(column, TOTAL_AGG_DOCS), String.valueOf(totalAggDocs));
    properties.setProperty(getKeyFor(column, DATA_TYPE), String.valueOf(fieldSpec.getDataType()));
    properties.setProperty(getKeyFor(column, BITS_PER_ELEMENT), String.valueOf(SingleValueUnsortedForwardIndexCreator.getNumOfBits(distinctValueCount)));
    properties.setProperty(getKeyFor(column, DICTIONARY_ELEMENT_SIZE), String.valueOf(dictionaryElementSize));
    properties.setProperty(getKeyFor(column, COLUMN_TYPE), String.valueOf(fieldSpec.getFieldType()));
    properties.setProperty(getKeyFor(column, IS_SORTED), String.valueOf(columnIndexCreationInfo.isSorted()));
    properties.setProperty(getKeyFor(column, HAS_NULL_VALUE), String.valueOf(columnIndexCreationInfo.hasNulls()));
    properties.setProperty(getKeyFor(column, HAS_DICTIONARY), String.valueOf(hasDictionary));
    properties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, HAS_INVERTED_INDEX),
        String.valueOf(hasInvertedIndex));
    properties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, IS_SINGLE_VALUED),
        String.valueOf(fieldSpec.isSingleValueField()));
    properties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, MAX_MULTI_VALUE_ELEMTS),
        String.valueOf(columnIndexCreationInfo.getMaxNumberOfMultiValueElements()));
    properties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, TOTAL_NUMBER_OF_ENTRIES),
        String.valueOf(columnIndexCreationInfo.getTotalNumberOfEntries()));
    properties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, IS_AUTO_GENERATED),
        String.valueOf(columnIndexCreationInfo.isAutoGenerated()));

    PartitionFunction partitionFunction = columnIndexCreationInfo.getPartitionFunction();
    int numPartitions = columnIndexCreationInfo.getNumPartitions();
    List<IntRange> partitionRanges = columnIndexCreationInfo.getPartitionRanges();
    if (partitionFunction != null && partitionRanges != null) {
      properties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, PARTITION_FUNCTION),
          partitionFunction.toString());
      properties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, NUM_PARTITIONS), numPartitions);
      String partitionValues = ColumnPartitionConfig.rangesToString(partitionRanges);
      properties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, PARTITION_VALUES), partitionValues);
    }

    // datetime field
    if (fieldSpec.getFieldType().equals(FieldType.DATE_TIME)) {
      DateTimeFieldSpec dateTimeFieldSpec = (DateTimeFieldSpec) fieldSpec;
      properties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, DATETIME_FORMAT),
          dateTimeFieldSpec.getFormat());
      properties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, DATETIME_GRANULARITY),
          dateTimeFieldSpec.getGranularity());
    }

    // HLL derived fields
    if (hllOriginColumn != null) {
      properties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, ORIGIN_COLUMN), hllOriginColumn);
      properties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, DERIVED_METRIC_TYPE), "HLL");
    }

    Object defaultNullValue = columnIndexCreationInfo.getDefaultNullValue();
    if (defaultNullValue == null) {
      defaultNullValue = fieldSpec.getDefaultNullValue();
    }
    properties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column, DEFAULT_NULL_VALUE),
        String.valueOf(defaultNullValue));
  }

  public static void addColumnMinMaxValueInfo(PropertiesConfiguration properties, String column, String minValue,
      String maxValue) {
    properties.setProperty(getKeyFor(column, MIN_VALUE), minValue);
    properties.setProperty(getKeyFor(column, MAX_VALUE), maxValue);
  }

  public static void removeColumnMetadataInfo(PropertiesConfiguration properties, String column) {
    properties.clearProperty(getKeyFor(column, CARDINALITY));
    properties.clearProperty(getKeyFor(column, TOTAL_DOCS));
    properties.clearProperty(getKeyFor(column, TOTAL_RAW_DOCS));
    properties.clearProperty(getKeyFor(column, TOTAL_AGG_DOCS));
    properties.clearProperty(getKeyFor(column, DATA_TYPE));
    properties.clearProperty(getKeyFor(column, BITS_PER_ELEMENT));
    properties.clearProperty(getKeyFor(column, DICTIONARY_ELEMENT_SIZE));
    properties.clearProperty(getKeyFor(column, COLUMN_TYPE));
    properties.clearProperty(getKeyFor(column, IS_SORTED));
    properties.clearProperty(getKeyFor(column, HAS_NULL_VALUE));
    properties.clearProperty(getKeyFor(column, HAS_DICTIONARY));
    properties.clearProperty(getKeyFor(column, HAS_INVERTED_INDEX));
    properties.clearProperty(getKeyFor(column, IS_SINGLE_VALUED));
    properties.clearProperty(getKeyFor(column, MAX_MULTI_VALUE_ELEMTS));
    properties.clearProperty(getKeyFor(column, TOTAL_NUMBER_OF_ENTRIES));
    properties.clearProperty(getKeyFor(column, IS_AUTO_GENERATED));
    properties.clearProperty(getKeyFor(column, DEFAULT_NULL_VALUE));
    properties.clearProperty(getKeyFor(column, DERIVED_METRIC_TYPE));
    properties.clearProperty(getKeyFor(column, ORIGIN_COLUMN));
    properties.clearProperty(getKeyFor(column, MIN_VALUE));
    properties.clearProperty(getKeyFor(column, MAX_VALUE));
  }

  /**
   * Helper method to build the raw index creator for the column.
   * Assumes that column to be indexed is single valued.
   *
   * @param file Output index file
   * @param column Column name
   * @param totalDocs Total number of documents to index
   * @param lengthOfLongestEntry Length of longest entry
   * @param mutableSegment
   * @return
   * @throws IOException
   */
  public static SingleValueRawIndexCreator getRawIndexCreatorForColumn(File file,
      ChunkCompressorFactory.CompressionType compressionType, String column, FieldSpec.DataType dataType, int totalDocs,
      int lengthOfLongestEntry, MutableSegmentImpl mutableSegment) throws IOException {

    SingleColumnSingleValueReader rawIndexReader = null;
    if (mutableSegment != null) {
      ColumnDataSource dataSource = mutableSegment.getDataSource(column);
      if (!dataSource.getDataSourceMetadata().hasDictionary()) {
        rawIndexReader = (SingleColumnSingleValueReader)dataSource.getForwardIndex();
      }
    }

    SingleValueRawIndexCreator indexCreator;
    switch (dataType) {
      case INT:
        indexCreator = new SingleValueFixedByteRawIndexCreator(file, compressionType, column, totalDocs, Integer.BYTES,
            dataType, rawIndexReader);
        break;

      case LONG:
        indexCreator = new SingleValueFixedByteRawIndexCreator(file, compressionType, column, totalDocs, Long.BYTES,
            dataType, rawIndexReader);
        break;

      case FLOAT:
        indexCreator = new SingleValueFixedByteRawIndexCreator(file, compressionType, column, totalDocs, Float.BYTES,
            dataType, rawIndexReader);
        break;

      case DOUBLE:
        indexCreator = new SingleValueFixedByteRawIndexCreator(file, compressionType, column, totalDocs, Double.BYTES,
            dataType, rawIndexReader);
        break;

      case STRING:
      case BYTES:
        indexCreator =
            new SingleValueVarByteRawIndexCreator(file, compressionType, column, totalDocs, lengthOfLongestEntry);
        break;

      default:
        throw new UnsupportedOperationException("Data type not supported for raw indexing: " + dataType);
    }

    return indexCreator;
  }

  @Override
  public void close() throws IOException {
    for (SegmentDictionaryCreator dictionaryCreator : _dictionaryCreatorMap.values()) {
      dictionaryCreator.close();
    }
    for (ForwardIndexCreator forwardIndexCreator : _forwardIndexCreatorMap.values()) {
      forwardIndexCreator.close();
    }
    for (InvertedIndexCreator invertedIndexCreator : _invertedIndexCreatorMap.values()) {
      invertedIndexCreator.close();
    }
  }
}
