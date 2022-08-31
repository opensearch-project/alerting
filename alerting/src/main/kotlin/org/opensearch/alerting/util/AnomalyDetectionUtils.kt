/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import org.apache.lucene.search.join.ScoreMode
import org.opensearch.common.Strings
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.SearchInput
import org.opensearch.commons.authuser.User
import org.opensearch.index.query.BoolQueryBuilder
import org.opensearch.index.query.NestedQueryBuilder
import org.opensearch.index.query.QueryBuilders
import org.opensearch.search.builder.SearchSourceBuilder

/**
 * AD monitor is search input monitor on top of anomaly result index. This method will return
 * true if monitor input only contains anomaly result index.
 */
fun isADMonitor(monitor: Monitor): Boolean {
    // If monitor has other input than AD result index, it's not AD monitor
    if (monitor.inputs.size != 1) {
        return false
    }
    val input = monitor.inputs[0]
    // AD monitor can only have 1 anomaly result index.
    if (input is SearchInput && input.indices.size == 1 && input.indices[0] == ".opendistro-anomaly-results*") {
        return true
    }
    return false
}

fun addUserBackendRolesFilter(user: User?, searchSourceBuilder: SearchSourceBuilder): SearchSourceBuilder {
    var boolQueryBuilder = BoolQueryBuilder()
    val userFieldName = "user"
    val userBackendRoleFieldName = "user.backend_roles.keyword"
    if (user == null || Strings.isEmpty(user.name)) {
        // For 1) old monitor and detector 2) security disabled or superadmin access, they have no/empty user field
        val userRolesFilterQuery = QueryBuilders.existsQuery(userFieldName)
        val nestedQueryBuilder = NestedQueryBuilder(userFieldName, userRolesFilterQuery, ScoreMode.None)
        boolQueryBuilder.mustNot(nestedQueryBuilder)
    } else if (user.backendRoles.isNullOrEmpty()) {
        // For simple FGAC user, they may have no backend roles, these users should be able to see detectors
        // of other users whose backend role is empty.
        val userRolesFilterQuery = QueryBuilders.existsQuery(userBackendRoleFieldName)
        val nestedQueryBuilder = NestedQueryBuilder(userFieldName, userRolesFilterQuery, ScoreMode.None)

        val userExistsQuery = QueryBuilders.existsQuery(userFieldName)
        val userExistsNestedQueryBuilder = NestedQueryBuilder(userFieldName, userExistsQuery, ScoreMode.None)

        boolQueryBuilder.mustNot(nestedQueryBuilder)
        boolQueryBuilder.must(userExistsNestedQueryBuilder)
    } else {
        // For normal case, user should have backend roles.
        val userRolesFilterQuery = QueryBuilders.termsQuery(userBackendRoleFieldName, user.backendRoles)
        val nestedQueryBuilder = NestedQueryBuilder(userFieldName, userRolesFilterQuery, ScoreMode.None)
        boolQueryBuilder.must(nestedQueryBuilder)
    }
    val query = searchSourceBuilder.query()
    if (query == null) {
        searchSourceBuilder.query(boolQueryBuilder)
    } else {
        (query as BoolQueryBuilder).filter(boolQueryBuilder)
    }
    return searchSourceBuilder
}
