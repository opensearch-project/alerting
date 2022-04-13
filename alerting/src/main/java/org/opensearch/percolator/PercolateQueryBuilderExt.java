/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.percolator;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.DelegatingAnalyzerWrapper;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.opensearch.OpenSearchException;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.analysis.FieldNameAnalyzer;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.mapper.SourceToParse;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.QueryShardException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.opensearch.search.SearchService.ALLOW_EXPENSIVE_QUERIES;

public class PercolateQueryBuilderExt extends PercolateQueryBuilder {
    public static final String NAME = "percolate_ext";
    private String field;
    private List<BytesReference> documents;
    private XContentType documentXContentType;


    public PercolateQueryBuilderExt(String field, List<BytesReference> documents, XContentType documentXContentType) {
        super(field, documents, documentXContentType);
        this.field = field;
        this.documents = documents;
        this.documentXContentType = documentXContentType;
    }

    public PercolateQueryBuilderExt(StreamInput sin) throws IOException {
        super(sin);
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        if (context.allowExpensiveQueries() == false) {
            throw new OpenSearchException(
                    "[percolate] queries cannot be executed when '" + ALLOW_EXPENSIVE_QUERIES.getKey() + "' is set to false."
            );
        }

        // Call nowInMillis() so that this query becomes un-cacheable since we
        // can't be sure that it doesn't use now or scripts
        context.nowInMillis();

        if (documents.isEmpty()) {
            throw new IllegalStateException("no document to percolate");
        }

        MappedFieldType fieldType = context.fieldMapper(field);
        if (fieldType == null) {
            throw new QueryShardException(context, "field [" + field + "] does not exist");
        }

        if (!(fieldType instanceof PercolatorFieldMapperExt.PercolatorFieldType)) {
            throw new QueryShardException(
                    context,
                    "expected field [" + field + "] to be of type [percolator], but is of type [" + fieldType.typeName() + "]"
            );
        }

        final List<ParsedDocument> docs = new ArrayList<>();
        final DocumentMapper docMapper;
        final MapperService mapperService = context.getMapperService();
        String type = mapperService.documentMapper().type();
        docMapper = mapperService.documentMapper();
        for (BytesReference document : documents) {
            docs.add(docMapper.parse(new SourceToParse(context.index().getName(), "_temp_id", document, documentXContentType)));
        }

        FieldNameAnalyzer fieldNameAnalyzer = (FieldNameAnalyzer) docMapper.mappers().indexAnalyzer();
        // Need to this custom impl because FieldNameAnalyzer is strict and the percolator sometimes isn't when
        // 'index.percolator.map_unmapped_fields_as_string' is enabled:
        Analyzer analyzer = new DelegatingAnalyzerWrapper(Analyzer.PER_FIELD_REUSE_STRATEGY) {
            @Override
            protected Analyzer getWrappedAnalyzer(String fieldName) {
                Analyzer analyzer = fieldNameAnalyzer.analyzers().get(fieldName);
                if (analyzer != null) {
                    return analyzer;
                } else {
                    return context.getIndexAnalyzers().getDefaultIndexAnalyzer();
                }
            }
        };
        final IndexSearcher docSearcher;
        final boolean excludeNestedDocuments;
        if (docs.size() > 1 || docs.get(0).docs().size() > 1) {
            assert docs.size() != 1 || docMapper.hasNestedObjects();
            docSearcher = createMultiDocumentSearcher(analyzer, docs);
            excludeNestedDocuments = docMapper.hasNestedObjects()
                    && docs.stream().map(ParsedDocument::docs).mapToInt(List::size).anyMatch(size -> size > 1);
        } else {
            MemoryIndex memoryIndex = MemoryIndex.fromDocument(docs.get(0).rootDoc(), analyzer, true, false);
            docSearcher = memoryIndex.createSearcher();
            docSearcher.setQueryCache(null);
            excludeNestedDocuments = false;
        }

        PercolatorFieldMapperExt.PercolatorFieldType pft = (PercolatorFieldMapperExt.PercolatorFieldType) fieldType;
        String name = pft.name();
        QueryShardContext percolateShardContext = wrap(context);
        PercolatorFieldMapper.configureContext(percolateShardContext, pft.mapUnmappedFieldsAsText);
        ;
        PercolateQuery.QueryStore queryStore = createStore(pft.queryBuilderField, percolateShardContext);

        return pft.percolateQuery(name, queryStore, documents, docSearcher, excludeNestedDocuments, context.indexVersionCreated());
    }
}