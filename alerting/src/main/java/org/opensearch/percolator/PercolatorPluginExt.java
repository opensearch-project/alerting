/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
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
