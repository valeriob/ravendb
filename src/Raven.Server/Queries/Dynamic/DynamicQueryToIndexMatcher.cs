﻿using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Server.Indexes;
using Raven.Server.Indexes.Auto;
using Raven.Server.Queries.Sort;

namespace Raven.Server.Queries.Dynamic
{
    public class DynamicQueryMatchResult
    {
        public string IndexName { get; set; }
        public DynamicQueryMatchType MatchType { get; set; }

        public DynamicQueryMatchResult(string match, DynamicQueryMatchType matchType)
        {
            this.IndexName = match;
            this.MatchType = matchType;
        }
    }

    public enum DynamicQueryMatchType
    {
        Complete,
        Partial,
        Failure
    }
    
    public class DynamicQueryToIndexMatcher
    {
        private readonly IndexStore _indexStore;

        public DynamicQueryToIndexMatcher(IndexStore indexStore)
        {
            _indexStore = indexStore;
        }

        private delegate void ExplainDelegate(string index, Func<string> rejectionReasonGenerator);

        public class Explanation
        {
            public string Index { get; set; }
            public string Reason { get; set; }
        }

        // TODO arek - definitely too big method
        public DynamicQueryMatchResult Match(DynamicQueryMapping query, List<Explanation> explanations = null)
        {
            if (query.MapFields.Length == 0 && // we optimize for empty queries to use Raven/DocumentsByEntityName
                (query.SortDescriptors.Length == 0) // && // and no sorting was requested
                )
                //_indexStore.Contains(Constants.DocumentsByEntityNameIndex)) // and Raven/DocumentsByEntityName exists
            {
                // TODO arek
                throw new NotImplementedException("We don't support empty dynamic qeries for now");
                //if (string.IsNullOrEmpty(query.ForCollection) == false)
                //    indexQuery.Query = "Tag:" + entityName;
                //return new DynamicQueryMatchResult(Constants.DocumentsByEntityNameIndex, DynamicQueryMatchType.Complete);
                //}
            }

            ExplainDelegate explain = (index, rejectionReason) => { };
            if (explanations != null)
            {
                explain = (index, rejectionReason) => explanations.Add(new Explanation
                {
                    Index = index,
                    Reason = rejectionReason()
                });
            }
            
            var autoIndexes = _indexStore.GetAutoIndexDefinitionsForCollection(query.ForCollection); // let us work with AutoIndexes only for now

            if (autoIndexes.Count == 0)
                return new DynamicQueryMatchResult(string.Empty, DynamicQueryMatchType.Failure);

            var results = autoIndexes.Select(definition => ConsiderUsageOfAutoIndex(definition, explain))
            .Where(result => result.MatchType != DynamicQueryMatchType.Failure)
                    .GroupBy(x => x.MatchType)
                    .ToDictionary(x => x.Key, x => x.ToArray());

            throw new NotImplementedException();


            //DynamicQueryMatchResult[] matchResults;
            //if (results.TryGetValue(DynamicQueryMatchType.Complete, out matchResults) && matchResults.Length > 0)
            //{
            //    DynamicQueryMatchResult[] prioritizedResults = null;
            //    database.TransactionalStorage.Batch(accessor =>
            //    {
            //        prioritizedResults = matchResults.OrderByDescending(result =>
            //        {
            //            var instance = this.database.IndexStorage.GetIndexInstance(result.IndexName);
            //            var stats = accessor.Indexing.GetIndexStats(instance.indexId);
            //            if (stats == null)
            //                return Etag.Empty;

            //            return stats.LastIndexedEtag;
            //        })
            //            .ThenByDescending(result =>
            //            {
            //                var abstractViewGenerator =
            //                    database.IndexDefinitionStorage.GetViewGenerator(result.IndexName);
            //                if (abstractViewGenerator == null)
            //                    return -1;
            //                return abstractViewGenerator.CountOfFields;
            //            })
            //            .ToArray();
            //    });
            //    for (int i = 1; i < prioritizedResults.Length; i++)
            //    {
            //        explain(prioritizedResults[i].IndexName,
            //                () => "Wasn't the widest / most unstable index matching this query");
            //    }

            //    return prioritizedResults[0];
            //}

            //if (results.TryGetValue(DynamicQueryMatchType.Partial, out matchResults) && matchResults.Length > 0)
            //{
            //    return matchResults.OrderByDescending(x =>
            //    {
            //        var viewGenerator = database.IndexDefinitionStorage.GetViewGenerator(x.IndexName);

            //        if (viewGenerator == null)
            //            return -1;
            //        return viewGenerator.CountOfFields;
            //    }).First();
            //}

            //return new DynamicQueryMatchResult("", DynamicQueryMatchType.Failure);

        }

        private DynamicQueryMatchResult ConsiderUsageOfAutoIndex(AutoIndexDefinition definition, ExplainDelegate explain)
        {
            var indexName = definition.Name;

            throw new NotImplementedException();
            //var abstractViewGenerator = database.IndexDefinitionStorage.GetViewGenerator(indexName);
            //var currentBestState = DynamicQueryMatchType.Complete;
            //if (abstractViewGenerator == null) // there is no matching view generator
            //{
            //    explain(indexName, () => "There is no matching view generator. Maybe the index in the process of being deleted?");
            //    return new DynamicQueryOptimizerResult(indexName, DynamicQueryMatchType.Failure);
            //}

            //if (definition.Value == null)
            //{
            //    explain(indexName, () => "Index id " + definition.Key + " is null, probably bad upgrade?");
            //    return new DynamicQueryOptimizerResult(indexName, DynamicQueryMatchType.Failure);
            //}

            //if (definition.Value.IsTestIndex)
            //{
            //    explain(indexName, () => "Cannot select a test index for dynamic query");
            //    return new DynamicQueryOptimizerResult(indexName, DynamicQueryMatchType.Failure);
            //}

            //var indexingPriority = IndexingPriority.None;
            //var isInvalidIndex = false;
            //database.TransactionalStorage.Batch(accessor =>
            //{
            //    var stats = accessor.Indexing.GetIndexStats(definition.Key);
            //    if (stats == null)
            //    {
            //        isInvalidIndex = true;
            //        return;
            //    }
            //    isInvalidIndex = stats.IsInvalidIndex;
            //    indexingPriority = stats.Priority;
            //});


            //if (entityName == null)
            //{
            //    if (abstractViewGenerator.ForEntityNames.Count != 0)
            //    {
            //        explain(indexName, () => "Query is not specific for entity name, but the index filter by entity names.");
            //        return new DynamicQueryOptimizerResult(indexName, DynamicQueryMatchType.Failure);
            //    }
            //}
            //else
            //{
            //    if (indexingPriority == IndexingPriority.Error ||
            //        indexingPriority == IndexingPriority.Disabled ||
            //        isInvalidIndex)
            //    {
            //        explain(indexName, () => string.Format("Cannot do dynamic queries on disabled index or index with errors (index name = {0})", indexName));
            //        return new DynamicQueryOptimizerResult(indexName, DynamicQueryMatchType.Failure);
            //    }

            //    if (abstractViewGenerator.ForEntityNames.Count > 1) // we only allow indexes with a single entity name
            //    {
            //        explain(indexName, () => "Index contains more than a single entity name, may result in a different type being returned.");
            //        return new DynamicQueryOptimizerResult(indexName, DynamicQueryMatchType.Failure);
            //    }
            //    if (abstractViewGenerator.ForEntityNames.Count == 0)
            //    {
            //        explain(indexName, () => "Query is specific for entity name, but the index searches across all of them, may result in a different type being returned.");
            //        return new DynamicQueryOptimizerResult(indexName, DynamicQueryMatchType.Failure);
            //    }
            //    if (abstractViewGenerator.ForEntityNames.Contains(entityName) == false) // for the specified entity name
            //    {
            //        explain(indexName, () => string.Format("Index does not apply to entity name: {0}", entityName));
            //        return new DynamicQueryOptimizerResult(indexName, DynamicQueryMatchType.Failure);
            //    }
            //}

            //if (abstractViewGenerator.ReduceDefinition != null) // we can't choose a map/reduce index
            //{
            //    explain(indexName, () => "Can't choose a map/reduce index for dynamic queries.");
            //    return new DynamicQueryOptimizerResult(indexName, DynamicQueryMatchType.Failure);
            //}

            //if (abstractViewGenerator.HasWhereClause) // without a where clause
            //{
            //    explain(indexName, () => "Can't choose an index with a where clause, it might filter things that the query is looking for.");
            //    return new DynamicQueryOptimizerResult(indexName, DynamicQueryMatchType.Failure);
            //}

            //// we can't select an index that has SelectMany in it, because it result in invalid results when
            //// you query it for things like Count, see https://github.com/ravendb/ravendb/issues/250
            //// for indexes with internal projections, we use the exact match based on the generated index name
            //// rather than selecting the optimal one
            //// in order to handle that, we count the number of select many that would happen because of the query
            //// and match it to the number of select many in the index
            //if (abstractViewGenerator.CountOfSelectMany != distinctSelectManyFields.Count)
            //{
            //    explain(indexName,
            //            () => "Can't choose an index with a different number of from clauses / SelectMany, will affect queries like Count().");
            //    return new DynamicQueryOptimizerResult(indexName, DynamicQueryMatchType.Failure);
            //}

            //if (normalizedFieldsQueriedUpon.All(abstractViewGenerator.ContainsFieldOnMap) == false)
            //{
            //    explain(indexName, () =>
            //    {
            //        var missingFields =
            //            normalizedFieldsQueriedUpon.Where(s => abstractViewGenerator.ContainsFieldOnMap(s) == false);
            //        return "The following fields are missing: " + string.Join(", ", missingFields);
            //    });
            //    currentBestState = DynamicQueryMatchType.Partial;
            //}

            //var indexDefinition = database.IndexDefinitionStorage.GetIndexDefinition(indexName);
            //if (indexDefinition == null)
            //    return new DynamicQueryOptimizerResult(indexName, DynamicQueryMatchType.Failure);

            //if (indexQuery.HighlightedFields != null && indexQuery.HighlightedFields.Length > 0)
            //{
            //    var nonHighlightableFields = indexQuery
            //        .HighlightedFields
            //        .Where(x =>
            //                !indexDefinition.Stores.ContainsKey(x.Field) ||
            //                indexDefinition.Stores[x.Field] != FieldStorage.Yes ||
            //                !indexDefinition.Indexes.ContainsKey(x.Field) ||
            //                indexDefinition.Indexes[x.Field] != FieldIndexing.Analyzed ||
            //                !indexDefinition.TermVectors.ContainsKey(x.Field) ||
            //                indexDefinition.TermVectors[x.Field] != FieldTermVector.WithPositionsAndOffsets)
            //        .Select(x => x.Field)
            //        .ToArray();

            //    if (nonHighlightableFields.Any())
            //    {
            //        explain(indexName,
            //            () => "The following fields could not be highlighted because they are not stored, analyzed and using term vectors with positions and offsets: " +
            //                  string.Join(", ", nonHighlightableFields));
            //        return new DynamicQueryOptimizerResult(indexName, DynamicQueryMatchType.Failure);
            //    }
            //}

            //if (indexQuery.SortedFields != null && indexQuery.SortedFields.Length > 0)
            //{
            //    var sortInfo = DynamicQueryMapping.GetSortInfo(s => { }, indexQuery);

            //    foreach (var sortedField in indexQuery.SortedFields) // with matching sort options
            //    {
            //        var sortField = sortedField.Field;
            //        if (sortField.StartsWith(Constants.AlphaNumericFieldName) ||
            //            sortField.StartsWith(Constants.RandomFieldName) ||
            //            sortField.StartsWith(Constants.CustomSortFieldName))
            //        {
            //            sortField = SortFieldHelper.CustomField(sortField).Name;
            //        }

            //        var normalizedFieldName = DynamicQueryMapping.ReplaceInvalidCharactersForFields(sortField);

            //        if (normalizedFieldName.EndsWith("_Range"))
            //            normalizedFieldName = normalizedFieldName.Substring(0, normalizedFieldName.Length - "_Range".Length);

            //        // if the field is not in the output, then we can't sort on it. 
            //        if (abstractViewGenerator.ContainsField(normalizedFieldName) == false)
            //        {
            //            explain(indexName,
            //                    () =>
            //                    "Rejected because index does not contains field '" + normalizedFieldName + "' which we need to sort on");
            //            currentBestState = DynamicQueryMatchType.Partial;
            //            continue;
            //        }

            //        var dynamicSortInfo = sortInfo.FirstOrDefault(x => x.Field == normalizedFieldName);

            //        if (dynamicSortInfo == null)// no sort order specified, we don't care, probably
            //            continue;

            //        SortOptions value;
            //        if (indexDefinition.SortOptions.TryGetValue(normalizedFieldName, out value) == false)
            //        {
            //            switch (dynamicSortInfo.FieldType)// if we can't find the value, we check if we asked for the default sorting
            //            {
            //                case SortOptions.String:
            //                case SortOptions.None:
            //                    continue;
            //                default:
            //                    explain(indexName,
            //                            () => "The specified sort type is different than the default for field: " + normalizedFieldName);
            //                    return new DynamicQueryOptimizerResult(indexName, DynamicQueryMatchType.Failure);
            //            }
            //        }

            //        if (value != dynamicSortInfo.FieldType)
            //        {
            //            explain(indexName,
            //                    () =>
            //                    "The specified sort type (" + dynamicSortInfo.FieldType + ") is different than the one specified for field '" +
            //                    normalizedFieldName + "' (" + value + ")");
            //            return new DynamicQueryOptimizerResult(indexName, DynamicQueryMatchType.Failure);
            //        }
            //    }
            //}

            //if (indexDefinition.Analyzers != null && indexDefinition.Analyzers.Count > 0)
            //{
            //    // none of the fields have custom analyzers
            //    if (normalizedFieldsQueriedUpon.Any(indexDefinition.Analyzers.ContainsKey))
            //    {
            //        explain(indexName, () =>
            //        {
            //            var fields = normalizedFieldsQueriedUpon.Where(indexDefinition.Analyzers.ContainsKey);
            //            return "The following field have a custom analyzer: " + string.Join(", ", fields);
            //        });
            //        return new DynamicQueryOptimizerResult(indexName, DynamicQueryMatchType.Failure);
            //    }
            //}

            //if (indexDefinition.Indexes != null && indexDefinition.Indexes.Count > 0)
            //{
            //    //If any of the fields we want to query on are set to something other than the default, don't use the index
            //    var anyFieldWithNonDefaultIndexing = normalizedFieldsQueriedUpon.Where(x =>
            //    {
            //        FieldIndexing analyzedInfo;
            //        if (indexDefinition.Indexes.TryGetValue(x, out analyzedInfo))
            //        {
            //            if (analyzedInfo != FieldIndexing.Default)
            //                return true;
            //        }
            //        return false;
            //    });

            //    var fieldWithNonDefaultIndexing = anyFieldWithNonDefaultIndexing.ToArray(); //prevent several enumerations
            //    if (fieldWithNonDefaultIndexing.Any())
            //    {
            //        explain(indexName, () =>
            //        {
            //            var fields = fieldWithNonDefaultIndexing.Where(indexDefinition.Analyzers.ContainsKey);
            //            return "The following field have aren't using default indexing: " + string.Join(", ", fields);
            //        });
            //        return new DynamicQueryOptimizerResult(indexName, DynamicQueryMatchType.Failure);
            //    }
            //}
            //if (currentBestState != DynamicQueryMatchType.Complete && indexDefinition.Type != "Auto")
            //    return new DynamicQueryOptimizerResult(indexName, DynamicQueryMatchType.Failure);
            //if (currentBestState == DynamicQueryMatchType.Complete &&
            //    (indexingPriority == IndexingPriority.Idle ||
            //     indexingPriority == IndexingPriority.Abandoned))
            //{
            //    currentBestState = DynamicQueryMatchType.Partial;
            //    explain(indexName, () => String.Format("The index (name = {0}) is disabled or abandoned. The preference is for active indexes - making a partial match", indexName));
            //}

            //return new DynamicQueryOptimizerResult(indexName, currentBestState);
        }
    }
}