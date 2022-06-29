/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.percolator;

import org.opensearch.common.settings.Setting;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.plugins.MapperPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.search.fetch.FetchSubPhase;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

/**
 * This is a stop-gap solution & will be removed in future.
 */
// TODO for cleanup
public class PercolatorPluginExt extends Plugin implements MapperPlugin, SearchPlugin, ExtensiblePlugin {
    @Override
    public List<QuerySpec<?>> getQueries() {
        return singletonList(new QuerySpec<>(PercolateQueryBuilderExt.NAME, PercolateQueryBuilderExt::new, PercolateQueryBuilderExt::fromXContent));
    }

    @Override
    public List<FetchSubPhase> getFetchSubPhases(FetchPhaseConstructionContext context) {
        return Arrays.asList(new PercolatorMatchedSlotSubFetchPhase(), new PercolatorHighlightSubFetchPhase(context.getHighlighters()));
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(PercolatorFieldMapperExt.INDEX_MAP_UNMAPPED_FIELDS_AS_TEXT_SETTING);
    }

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return singletonMap(PercolatorFieldMapperExt.CONTENT_TYPE, new PercolatorFieldMapperExt.TypeParser());
    }

    @Override
    public void loadExtensions(ExtensionLoader loader) {
        ExtensiblePlugin.super.loadExtensions(loader);
    }
}
