package org.opensearch.alerting.resthandler

import org.apache.http.HttpHeaders
import org.apache.http.message.BasicHeader
import org.opensearch.alerting.*
import org.opensearch.alerting.core.model.Input
import org.opensearch.alerting.core.model.IntervalSchedule
import org.opensearch.alerting.core.model.Schedule
import org.opensearch.alerting.core.model.SearchInput
import org.opensearch.alerting.model.Monitor
import org.opensearch.alerting.model.QueryLevelTrigger
import org.opensearch.alerting.model.Trigger
import org.opensearch.commons.authuser.User
import org.opensearch.index.query.QueryBuilders
import org.opensearch.rest.RestStatus
import org.opensearch.script.Script
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.test.rest.OpenSearchRestTestCase
import java.time.Instant
import java.time.temporal.ChronoUnit

class MonitorRestApilTForDocument: AlertingRestTestCase() {



}
