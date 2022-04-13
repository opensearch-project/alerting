/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.alerting

import org.opensearch.alerting.core.model.Input
import org.opensearch.alerting.core.model.IntervalSchedule
import org.opensearch.alerting.core.model.Schedule
import org.opensearch.alerting.core.model.SearchInput
import org.opensearch.alerting.model.Monitor
import org.opensearch.alerting.model.QueryLevelTrigger
import org.opensearch.alerting.model.Trigger
import org.opensearch.commons.authuser.User
import org.opensearch.index.query.BoolQueryBuilder
import org.opensearch.index.query.QueryBuilders
import org.opensearch.script.Script
import org.opensearch.search.aggregations.AggregationBuilders
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.test.rest.OpenSearchRestTestCase
import java.time.Instant
import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit

const val ANOMALY_DETECTOR_INDEX = ".opendistro-anomaly-detectors"
const val ANOMALY_RESULT_INDEX = ".opendistro-anomaly-results*"

fun anomalyDetectorIndexMapping(): String {
    return """
        "properties": {
    "schema_version": {
      "type": "integer"
    },
    "name": {
      "type": "text",
      "fields": {
        "keyword": {
          "type": "keyword",
          "ignore_above": 256
        }
      }
    },
    "description": {
      "type": "text"
    },
    "time_field": {
      "type": "keyword"
    },
    "indices": {
      "type": "text",
      "fields": {
        "keyword": {
          "type": "keyword",
          "ignore_above": 256
        }
      }
    },
    "filter_query": {
      "type": "object",
      "enabled": false
    },
    "feature_attributes": {
      "type": "nested",
      "properties": {
        "feature_id": {
          "type": "keyword",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "feature_name": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "feature_enabled": {
          "type": "boolean"
        },
        "aggregation_query": {
          "type": "object",
          "enabled": false
        }
      }
    },
    "detection_interval": {
      "properties": {
        "period": {
          "properties": {
            "interval": {
              "type": "integer"
            },
            "unit": {
              "type": "keyword"
            }
          }
        }
      }
    },
    "window_delay": {
      "properties": {
        "period": {
          "properties": {
            "interval": {
              "type": "integer"
            },
            "unit": {
              "type": "keyword"
            }
          }
        }
      }
    },
    "shingle_size": {
      "type": "integer"
    },
    "last_update_time": {
      "type": "date",
      "format": "strict_date_time||epoch_millis"
    },
    "ui_metadata": {
      "type": "object",
      "enabled": false
    },
    "user": {
      "type": "nested",
      "properties": {
        "name": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "backend_roles": {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword"
            }
          }
        },
        "roles": {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword"
            }
          }
        },
        "custom_attribute_names": {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword"
            }
          }
        }
      }
    },
    "category_field": {
      "type": "keyword"
    }
  }
        """
}

fun anomalyResultIndexMapping(): String {
    return """
            "properties": {
              "detector_id": {
                "type": "keyword"
              },
              "is_anomaly": {
                "type": "boolean"
              },
              "anomaly_score": {
                "type": "double"
              },
              "anomaly_grade": {
                "type": "double"
              },
              "confidence": {
                "type": "double"
              },
              "feature_data": {
                "type": "nested",
                "properties": {
                  "feature_id": {
                    "type": "keyword"
                  },
                  "data": {
                    "type": "double"
                  }
                }
              },
              "data_start_time": {
                "type": "date",
                "format": "strict_date_time||epoch_millis"
              },
              "data_end_time": {
                "type": "date",
                "format": "strict_date_time||epoch_millis"
              },
              "execution_start_time": {
                "type": "date",
                "format": "strict_date_time||epoch_millis"
              },
              "execution_end_time": {
                "type": "date",
                "format": "strict_date_time||epoch_millis"
              },
              "error": {
                "type": "text"
              },
              "user": {
                "type": "nested",
                "properties": {
                  "name": {
                    "type": "text",
                    "fields": {
                      "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                      }
                    }
                  },
                  "backend_roles": {
                    "type": "text",
                    "fields": {
                      "keyword": {
                        "type": "keyword"
                      }
                    }
                  },
                  "roles": {
                    "type": "text",
                    "fields": {
                      "keyword": {
                        "type": "keyword"
                      }
                    }
                  },
                  "custom_attribute_names": {
                    "type": "text",
                    "fields": {
                      "keyword": {
                        "type": "keyword"
                      }
                    }
                  }
                }
              },
              "entity": {
                "type": "nested",
                "properties": {
                  "name": {
                    "type": "keyword"
                  },
                  "value": {
                    "type": "keyword"
                  }
                }
              },
              "schema_version": {
                "type": "integer"
              }
            }
        """
}

fun randomAnomalyDetector(): String {
    return """{
    "name" : "${OpenSearchTestCase.randomAlphaOfLength(10)}",
    "description" : "${OpenSearchTestCase.randomAlphaOfLength(10)}",
    "time_field" : "timestamp",
    "indices" : [
      "${OpenSearchTestCase.randomAlphaOfLength(5)}"
    ],
    "filter_query" : {
      "match_all" : {
        "boost" : 1.0
      }
    },
    "detection_interval" : {
      "period" : {
        "interval" : 1,
        "unit" : "Minutes"
      }
    },
    "window_delay" : {
      "period" : {
        "interval" : 1,
        "unit" : "Minutes"
      }
    },
    "shingle_size" : 8,
    "feature_attributes" : [
      {
        "feature_name" : "F1",
        "feature_enabled" : true,
        "aggregation_query" : {
          "f_1" : {
            "sum" : {
              "field" : "value"
            }
          }
        }
      }
    ]
  }
    """.trimIndent()
}

fun randomAnomalyDetectorWithUser(backendRole: String): String {
    return """{
    "name" : "${OpenSearchTestCase.randomAlphaOfLength(5)}",
    "description" : "${OpenSearchTestCase.randomAlphaOfLength(10)}",
    "time_field" : "timestamp",
    "indices" : [
      "${OpenSearchTestCase.randomAlphaOfLength(5)}"
    ],
    "filter_query" : {
      "match_all" : {
        "boost" : 1.0
      }
    },
    "detection_interval" : {
      "period" : {
        "interval" : 1,
        "unit" : "Minutes"
      }
    },
    "window_delay" : {
      "period" : {
        "interval" : 1,
        "unit" : "Minutes"
      }
    },
    "shingle_size" : 8,
    "feature_attributes" : [
      {
        "feature_name" : "F1",
        "feature_enabled" : true,
        "aggregation_query" : {
          "f_1" : {
            "sum" : {
              "field" : "value"
            }
          }
        }
      }
    ],
    "user" : {
      "name" : "${OpenSearchTestCase.randomAlphaOfLength(5)}",
      "backend_roles" : [ "$backendRole" ],
      "roles" : [
        "${OpenSearchTestCase.randomAlphaOfLength(5)}"
      ],
      "custom_attribute_names" : [ ]
    }
  }
    """.trimIndent()
}

fun randomAnomalyResult(
    detectorId: String = OpenSearchTestCase.randomAlphaOfLength(10),
    dataStartTime: Long = ZonedDateTime.now().minus(2, ChronoUnit.MINUTES).toInstant().toEpochMilli(),
    dataEndTime: Long = ZonedDateTime.now().toInstant().toEpochMilli(),
    featureId: String = OpenSearchTestCase.randomAlphaOfLength(5),
    featureName: String = OpenSearchTestCase.randomAlphaOfLength(5),
    featureData: Double = OpenSearchTestCase.randomDouble(),
    executionStartTime: Long = ZonedDateTime.now().minus(10, ChronoUnit.SECONDS).toInstant().toEpochMilli(),
    executionEndTime: Long = ZonedDateTime.now().toInstant().toEpochMilli(),
    anomalyScore: Double = OpenSearchTestCase.randomDouble(),
    anomalyGrade: Double = OpenSearchTestCase.randomDouble(),
    confidence: Double = OpenSearchTestCase.randomDouble(),
    user: User = randomUser()
): String {
    return """{
          "detector_id" : "$detectorId",
          "data_start_time" : $dataStartTime,
          "data_end_time" : $dataEndTime,
          "feature_data" : [
            {
              "feature_id" : "$featureId",
              "feature_name" : "$featureName",
              "data" : $featureData
            }
          ],
          "execution_start_time" : $executionStartTime,
          "execution_end_time" : $executionEndTime,
          "anomaly_score" : $anomalyScore,
          "anomaly_grade" : $anomalyGrade,
          "confidence" : $confidence,
          "user" : {
            "name" : "${user.name}",
            "backend_roles" : [
              ${user.backendRoles.joinToString { "\"${it}\"" }}
            ],
            "roles" : [
              ${user.roles.joinToString { "\"${it}\"" }}
            ],
            "custom_attribute_names" : [
              ${user.customAttNames.joinToString { "\"${it}\"" }}
            ]
          }
        }
    """.trimIndent()
}

fun randomAnomalyResultWithoutUser(
    detectorId: String = OpenSearchTestCase.randomAlphaOfLength(10),
    dataStartTime: Long = ZonedDateTime.now().minus(2, ChronoUnit.MINUTES).toInstant().toEpochMilli(),
    dataEndTime: Long = ZonedDateTime.now().toInstant().toEpochMilli(),
    featureId: String = OpenSearchTestCase.randomAlphaOfLength(5),
    featureName: String = OpenSearchTestCase.randomAlphaOfLength(5),
    featureData: Double = OpenSearchTestCase.randomDouble(),
    executionStartTime: Long = ZonedDateTime.now().minus(10, ChronoUnit.SECONDS).toInstant().toEpochMilli(),
    executionEndTime: Long = ZonedDateTime.now().toInstant().toEpochMilli(),
    anomalyScore: Double = OpenSearchTestCase.randomDouble(),
    anomalyGrade: Double = OpenSearchTestCase.randomDouble(),
    confidence: Double = OpenSearchTestCase.randomDouble()
): String {
    return """{
          "detector_id" : "$detectorId",
          "data_start_time" : $dataStartTime,
          "data_end_time" : $dataEndTime,
          "feature_data" : [
            {
              "feature_id" : "$featureId",
              "feature_name" : "$featureName",
              "data" : $featureData
            }
          ],
          "execution_start_time" : $executionStartTime,
          "execution_end_time" : $executionEndTime,
          "anomaly_score" : $anomalyScore,
          "anomaly_grade" : $anomalyGrade,
          "confidence" : $confidence
        }
    """.trimIndent()
}

fun maxAnomalyGradeSearchInput(
    adResultIndex: String = ".opendistro-anomaly-results-history",
    detectorId: String = OpenSearchTestCase.randomAlphaOfLength(10),
    size: Int = 1
): SearchInput {
    val rangeQuery = QueryBuilders.rangeQuery("execution_end_time")
        .gt("{{period_end}}||-10d")
        .lte("{{period_end}}")
        .format("epoch_millis")
    val termQuery = QueryBuilders.termQuery("detector_id", detectorId)

    var boolQueryBuilder = BoolQueryBuilder()
    boolQueryBuilder.filter(rangeQuery).filter(termQuery)

    val aggregationBuilder = AggregationBuilders.max("max_anomaly_grade").field("anomaly_grade")
    val searchSourceBuilder = SearchSourceBuilder().query(boolQueryBuilder).aggregation(aggregationBuilder).size(size)
    return SearchInput(indices = listOf(adResultIndex), query = searchSourceBuilder)
}

fun adMonitorTrigger(): QueryLevelTrigger {
    val triggerScript = """
            return ctx.results[0].aggregations.max_anomaly_grade.value != null && 
                   ctx.results[0].aggregations.max_anomaly_grade.value > 0.7
    """.trimIndent()
    return randomQueryLevelTrigger(condition = Script(triggerScript))
}

fun adSearchInput(detectorId: String): SearchInput {
    return maxAnomalyGradeSearchInput(adResultIndex = ANOMALY_RESULT_INDEX, detectorId = detectorId, size = 10)
}

fun randomADMonitor(
    name: String = OpenSearchRestTestCase.randomAlphaOfLength(10),
    user: User? = randomUser(),
    inputs: List<Input> = listOf(adSearchInput("test_detector_id")),
    schedule: Schedule = IntervalSchedule(interval = 5, unit = ChronoUnit.MINUTES),
    enabled: Boolean = OpenSearchTestCase.randomBoolean(),
    triggers: List<Trigger> = (1..OpenSearchTestCase.randomInt(10)).map { randomQueryLevelTrigger() },
    enabledTime: Instant? = if (enabled) Instant.now().truncatedTo(ChronoUnit.MILLIS) else null,
    lastUpdateTime: Instant = Instant.now().truncatedTo(ChronoUnit.MILLIS),
    withMetadata: Boolean = false
): Monitor {
    return Monitor(
        name = name, monitorType = Monitor.MonitorType.QUERY_LEVEL_MONITOR, enabled = enabled, inputs = inputs,
        schedule = schedule, triggers = triggers, enabledTime = enabledTime, lastUpdateTime = lastUpdateTime,
        user = user, uiMetadata = if (withMetadata) mapOf("foo" to "bar") else mapOf()
    )
}

fun randomADUser(backendRole: String = OpenSearchRestTestCase.randomAlphaOfLength(10)): User {
    return User(
        OpenSearchRestTestCase.randomAlphaOfLength(10), listOf(backendRole),
        listOf(OpenSearchRestTestCase.randomAlphaOfLength(10), ALL_ACCESS_ROLE), listOf("test_attr=test")
    )
}
