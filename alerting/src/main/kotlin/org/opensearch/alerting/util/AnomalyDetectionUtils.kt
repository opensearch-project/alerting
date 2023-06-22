/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import org.apache.lucene.search.join.ScoreMode
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.SearchInput
import org.opensearch.commons.authuser.User
import org.opensearch.core.common.Strings
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

fun getADBackendRoleFilterEnabled(clusterService: ClusterService, settings: Settings): Boolean {
    var adBackendRoleFilterEnabled: Boolean
    val metaData = clusterService.state().metadata()
    val adFilterString = "plugins.anomaly_detection.filter_by_backend_roles"

    // get default value for setting
    if (clusterService.clusterSettings.get(adFilterString) != null) {
        adBackendRoleFilterEnabled = clusterService.clusterSettings.get(adFilterString).getDefault(settings) as Boolean
    } else {
        // default setting doesn't exist, so returning false as it means AD plugins isn't in cluster anyway
        return false
    }

    // Transient settings are prioritized so those are checked first.
    return if (metaData.transientSettings().get(adFilterString) != null) {
        metaData.transientSettings().getAsBoolean(adFilterString, adBackendRoleFilterEnabled)
    } else if (metaData.persistentSettings().get(adFilterString) != null) {
        metaData.persistentSettings().getAsBoolean(adFilterString, adBackendRoleFilterEnabled)
    } else {
        adBackendRoleFilterEnabled
    }
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
