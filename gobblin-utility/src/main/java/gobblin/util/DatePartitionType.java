/*
 * Copyright (C) 2016 Lorand Bendig All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.util;

import java.util.LinkedHashMap;
import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.DateTimeFieldType;


/**
 * Temporal granularity types for writing ({@link gobblin.writer.partitioner.TimeBasedWriterPartitioner}) and reading
 * ({@link gobblin.source.DatePartitionedAvroFileSource}) date partitioned data.
 * 
 * @author Lorand Bendig
 *
 */
public enum DatePartitionType {

  YEAR("yyyy", DateTimeFieldType.year()),
  MONTH("yyyy/MM", DateTimeFieldType.monthOfYear()),
  DAY("yyyy/MM/dd", DateTimeFieldType.dayOfMonth()),
  HOUR("yyyy/MM/dd/HH", DateTimeFieldType.hourOfDay()),
  MINUTE("yyyy/MM/dd/HH/mm", DateTimeFieldType.minuteOfHour());

  private static final Map<String, DateTimeFieldType> lookupByPattern = new LinkedHashMap<>();

  static {
    lookupByPattern.put("s", DateTimeFieldType.secondOfMinute());
    lookupByPattern.put("m", DateTimeFieldType.minuteOfHour());
    lookupByPattern.put("h", DateTimeFieldType.hourOfDay());
    lookupByPattern.put("H", DateTimeFieldType.hourOfDay());
    lookupByPattern.put("K", DateTimeFieldType.hourOfDay());
    lookupByPattern.put("d", DateTimeFieldType.dayOfMonth());
    lookupByPattern.put("D", DateTimeFieldType.dayOfMonth());
    lookupByPattern.put("e", DateTimeFieldType.dayOfMonth());

    lookupByPattern.put("w", DateTimeFieldType.weekOfWeekyear());
    lookupByPattern.put("M", DateTimeFieldType.monthOfYear());
    lookupByPattern.put("y", DateTimeFieldType.year());
    lookupByPattern.put("Y", DateTimeFieldType.year());
  }

  private DateTimeFieldType dateTimeField;
  private String dateTimePattern;

  private DatePartitionType(String dateTimePattern, DateTimeFieldType dateTimeField) {
    this.dateTimeField = dateTimeField;
    this.dateTimePattern = dateTimePattern;
  }

  /**
   * @param pattern full partitioning pattern
   * @return a DateTimeFieldType corresponding to the smallest temporal unit in the pattern. 
   * E.g for yyyy/MM/dd {@link DateTimeFieldType#dayOfMonth()}
   */
  public static DateTimeFieldType getLowestIntervalUnit(String pattern) {
    DateTimeFieldType intervalUnit = null;
    for (Map.Entry<String, DateTimeFieldType> pat : lookupByPattern.entrySet()) {
      if (pattern.contains(pat.getKey())) {
        intervalUnit = pat.getValue();
        break;
      }
    }
    return intervalUnit;
  }
  
  public DateTimeFieldType getDateTimeFieldType() {
    return dateTimeField;
  }
  
  public int getField(DateTime dateTime) {
    return dateTime.get(this.dateTimeField);
  }
  
  public String getDateTimePattern() {
    return dateTimePattern;
  }

}