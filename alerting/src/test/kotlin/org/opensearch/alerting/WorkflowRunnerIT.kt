/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.junit.Assert
import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.model.DocumentLevelTriggerRunResult
import org.opensearch.alerting.transport.WorkflowSingleNodeTestCase
import org.opensearch.alerting.util.AlertingException
import org.opensearch.commons.alerting.action.AcknowledgeAlertRequest
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.GetAlertsRequest
import org.opensearch.commons.alerting.action.IndexMonitorResponse
import org.opensearch.commons.alerting.aggregation.bucketselectorext.BucketSelectorExtAggregationBuilder
import org.opensearch.commons.alerting.model.DataSources
import org.opensearch.commons.alerting.model.DocLevelMonitorInput
import org.opensearch.commons.alerting.model.DocLevelQuery
import org.opensearch.commons.alerting.model.SearchInput
import org.opensearch.commons.alerting.model.Table
import org.opensearch.index.query.QueryBuilders
import org.opensearch.rest.RestStatus
import org.opensearch.script.Script
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder
import org.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder
import org.opensearch.search.builder.SearchSourceBuilder
import java.lang.Exception
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.concurrent.ExecutionException

class WorkflowRunnerIT : WorkflowSingleNodeTestCase() {

    fun `test execute workflow with custom alerts and finding index with doc level delegates`() {
        val docQuery1 = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3")
        val docLevelInput1 = DocLevelMonitorInput("description", listOf(index), listOf(docQuery1))
        val trigger1 = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val customAlertsIndex1 = "custom_alerts_index"
        val customFindingsIndex1 = "custom_findings_index"
        val customFindingsIndexPattern1 = "custom_findings_index-1"
        var monitor1 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput1),
            triggers = listOf(trigger1),
            dataSources = DataSources(
                alertsIndex = customAlertsIndex1,
                findingsIndex = customFindingsIndex1,
                findingsIndexPattern = customFindingsIndexPattern1
            )
        )
        val monitorResponse = createMonitor(monitor1)!!

        val docQuery2 = DocLevelQuery(query = "source.ip.v6.v2:16645", name = "4")
        val docLevelInput2 = DocLevelMonitorInput("description", listOf(index), listOf(docQuery2))
        val trigger2 = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val customAlertsIndex2 = "custom_alerts_index_2"
        val customFindingsIndex2 = "custom_findings_index_2"
        val customFindingsIndexPattern2 = "custom_findings_index-2"
        var monitor2 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput2),
            triggers = listOf(trigger2),
            dataSources = DataSources(
                alertsIndex = customAlertsIndex2,
                findingsIndex = customFindingsIndex2,
                findingsIndexPattern = customFindingsIndexPattern2
            )
        )

        val monitorResponse2 = createMonitor(monitor2)!!

        var workflow = randomWorkflowMonitor(
            monitorIds = listOf(monitorResponse.id, monitorResponse2.id)
        )
        val workflowResponse = upsertWorkflow(workflow)!!
        val workflowById = searchWorkflow(workflowResponse.id)!!
        assertNotNull(workflowById)

        var testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(ChronoUnit.MILLIS))
        // Matches monitor1
        val testDoc1 = """{
            "message" : "This is an error from IAD region",
            "source.ip.v6.v2" : 16644, 
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""
        indexDoc(index, "1", testDoc1)

        testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(ChronoUnit.MILLIS))
        // Matches monitor1 and monitor2
        val testDoc2 = """{
            "message" : "This is an error from IAD region",
            "source.ip.v6.v2" : 16645, 
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""
        indexDoc(index, "2", testDoc2)

        testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(ChronoUnit.MILLIS))
        // Doesn't match
        val testDoc3 = """{
            "message" : "This is an error from IAD region",
            "source.ip.v6.v2" : 16645, 
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-east-1"
        }"""
        indexDoc(index, "3", testDoc3)

        val workflowId = workflowResponse.id
        val executeWorkflowResponse = executeWorkflow(workflowById, workflowId, false)!!
        val monitorsRunResults = executeWorkflowResponse.workflowRunResult.workflowRunResult
        assertEquals(2, monitorsRunResults.size)

        assertEquals(monitor1.name, monitorsRunResults[0].monitorName)
        assertEquals(1, monitorsRunResults[0].triggerResults.size)

        Assert.assertEquals(monitor2.name, monitorsRunResults[1].monitorName)
        Assert.assertEquals(1, monitorsRunResults[1].triggerResults.size)

        assertAlerts(monitorResponse, customAlertsIndex1, 2)
        assertFindings(monitorResponse.id, customFindingsIndex1, 2, 2, listOf("1", "2"))

        assertAlerts(monitorResponse2, customAlertsIndex2, 1)
        assertFindings(monitorResponse2.id, customFindingsIndex2, 1, 1, listOf("2"))
    }

    fun `test execute workflow with custom alerts and finding index with bucket level doc level delegates when bucket level delegate is used in chained finding`() {
        val query = QueryBuilders.rangeQuery("test_strict_date_time")
            .gt("{{period_end}}||-10d")
            .lte("{{period_end}}")
            .format("epoch_millis")
        val compositeSources = listOf(
            TermsValuesSourceBuilder("test_field").field("test_field")
        )
        val compositeAgg = CompositeAggregationBuilder("composite_agg", compositeSources)
        val input = SearchInput(indices = listOf(index), query = SearchSourceBuilder().size(0).query(query).aggregation(compositeAgg))
        // Bucket level monitor will reduce the size of matched doc ids on those that belong to a bucket that contains more than 1 document after term grouping
        val triggerScript = """
            params.docCount > 1
        """.trimIndent()

        var trigger = randomBucketLevelTrigger()
        trigger = trigger.copy(
            bucketSelector = BucketSelectorExtAggregationBuilder(
                name = trigger.id,
                bucketsPathsMap = mapOf("docCount" to "_count"),
                script = Script(triggerScript),
                parentBucketPath = "composite_agg",
                filter = null,
            )
        )
        val bucketCustomAlertsIndex = "custom_alerts_index"
        val bucketCustomFindingsIndex = "custom_findings_index"
        val bucketCustomFindingsIndexPattern = "custom_findings_index-1"

        val bucketLevelMonitorResponse = createMonitor(
            randomBucketLevelMonitor(
                inputs = listOf(input),
                enabled = false,
                triggers = listOf(trigger),
                dataSources = DataSources(
                    findingsEnabled = true,
                    alertsIndex = bucketCustomAlertsIndex,
                    findingsIndex = bucketCustomFindingsIndex,
                    findingsIndexPattern = bucketCustomFindingsIndexPattern
                )
            )
        )!!

        val docQuery1 = DocLevelQuery(query = "test_field:\"test_value_2\"", name = "1")
        val docQuery2 = DocLevelQuery(query = "test_field:\"test_value_1\"", name = "2")
        val docQuery3 = DocLevelQuery(query = "test_field:\"test_value_3\"", name = "3")
        val docLevelInput = DocLevelMonitorInput("description", listOf(index), listOf(docQuery1, docQuery2, docQuery3))
        val docTrigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val docCustomAlertsIndex = "custom_alerts_index"
        val docCustomFindingsIndex = "custom_findings_index"
        val docCustomFindingsIndexPattern = "custom_findings_index-1"
        var docLevelMonitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(docTrigger),
            dataSources = DataSources(
                alertsIndex = docCustomAlertsIndex,
                findingsIndex = docCustomFindingsIndex,
                findingsIndexPattern = docCustomFindingsIndexPattern
            )
        )

        val docLevelMonitorResponse = createMonitor(docLevelMonitor)!!
        // 1. bucketMonitor (chainedFinding = null) 2. docMonitor (chainedFinding = bucketMonitor)
        var workflow = randomWorkflowMonitor(
            monitorIds = listOf(bucketLevelMonitorResponse.id, docLevelMonitorResponse.id)
        )
        val workflowResponse = upsertWorkflow(workflow)!!
        val workflowById = searchWorkflow(workflowResponse.id)!!
        assertNotNull(workflowById)

        // Creates 5 documents
        insertSampleTimeSerializedData(
            index,
            listOf(
                "test_value_1",
                "test_value_1", // adding duplicate to verify aggregation
                "test_value_2",
                "test_value_2",
                "test_value_3"
            )
        )

        val workflowId = workflowResponse.id
        // 1. bucket level monitor should reduce the doc findings to 4 (1, 2, 3, 4)
        // 2. Doc level monitor will match those 4 documents although it contains rules for matching all 5 documents (docQuery3 matches the fifth)
        val executeWorkflowResponse = executeWorkflow(workflowById, workflowId, false)!!
        assertNotNull(executeWorkflowResponse)

        for (monitorRunResults in executeWorkflowResponse.workflowRunResult.workflowRunResult) {
            if (bucketLevelMonitorResponse.monitor.name == monitorRunResults.monitorName) {
                val searchResult = monitorRunResults.inputResults.results.first()
                @Suppress("UNCHECKED_CAST")
                val buckets = searchResult.stringMap("aggregations")?.stringMap("composite_agg")?.get("buckets") as List<Map<String, Any>>
                assertEquals("Incorrect search result", 3, buckets.size)

                assertAlerts(bucketLevelMonitorResponse, bucketCustomAlertsIndex, 2)
                assertFindings(bucketLevelMonitorResponse.id, bucketCustomFindingsIndex, 1, 4, listOf("1", "2", "3", "4"))
            } else {
                assertEquals(1, monitorRunResults.inputResults.results.size)
                val values = monitorRunResults.triggerResults.values
                assertEquals(1, values.size)
                @Suppress("UNCHECKED_CAST")
                val docLevelTrigger = values.iterator().next() as DocumentLevelTriggerRunResult
                val triggeredDocIds = docLevelTrigger.triggeredDocs.map { it.split("|")[0] }
                val expectedTriggeredDocIds = listOf("1", "2", "3", "4")
                assertEquals(expectedTriggeredDocIds, triggeredDocIds.sorted())

                assertAlerts(docLevelMonitorResponse, docCustomAlertsIndex, 4)
                assertFindings(docLevelMonitorResponse.id, docCustomFindingsIndex, 4, 4, listOf("1", "2", "3", "4"))
            }
        }
    }

    fun `test execute workflow with custom alerts and finding index with bucket level and doc level delegates when doc level delegate is used in chained finding`() {
        val docQuery1 = DocLevelQuery(query = "test_field:\"test_value_2\"", name = "1")
        val docQuery2 = DocLevelQuery(query = "test_field:\"test_value_3\"", name = "2")

        var docLevelMonitor = randomDocumentLevelMonitor(
            inputs = listOf(DocLevelMonitorInput("description", listOf(index), listOf(docQuery1, docQuery2))),
            triggers = listOf(randomDocumentLevelTrigger(condition = ALWAYS_RUN)),
            dataSources = DataSources(
                alertsIndex = "custom_alerts_index",
                findingsIndex = "custom_findings_index",
                findingsIndexPattern = "custom_findings_index-1"
            )
        )

        val docLevelMonitorResponse = createMonitor(docLevelMonitor)!!

        val query = QueryBuilders.rangeQuery("test_strict_date_time")
            .gt("{{period_end}}||-10d")
            .lte("{{period_end}}")
            .format("epoch_millis")
        val compositeSources = listOf(
            TermsValuesSourceBuilder("test_field").field("test_field")
        )
        val compositeAgg = CompositeAggregationBuilder("composite_agg", compositeSources)
        val input = SearchInput(indices = listOf(index), query = SearchSourceBuilder().size(0).query(query).aggregation(compositeAgg))
        // Bucket level monitor will reduce the size of matched doc ids on those that belong to a bucket that contains more than 1 document after term grouping
        val triggerScript = """
            params.docCount > 1
        """.trimIndent()

        var trigger = randomBucketLevelTrigger()
        trigger = trigger.copy(
            bucketSelector = BucketSelectorExtAggregationBuilder(
                name = trigger.id,
                bucketsPathsMap = mapOf("docCount" to "_count"),
                script = Script(triggerScript),
                parentBucketPath = "composite_agg",
                filter = null,
            )
        )

        val bucketLevelMonitorResponse = createMonitor(
            randomBucketLevelMonitor(
                inputs = listOf(input),
                enabled = false,
                triggers = listOf(trigger),
                dataSources = DataSources(
                    findingsEnabled = true,
                    alertsIndex = "custom_alerts_index",
                    findingsIndex = "custom_findings_index",
                    findingsIndexPattern = "custom_findings_index-1"
                )
            )
        )!!

        var docLevelMonitor1 = randomDocumentLevelMonitor(
            // Match the documents with test_field: test_value_3
            inputs = listOf(DocLevelMonitorInput("description", listOf(index), listOf(docQuery2))),
            triggers = listOf(randomDocumentLevelTrigger(condition = ALWAYS_RUN)),
            dataSources = DataSources(
                findingsEnabled = true,
                alertsIndex = "custom_alerts_index_1",
                findingsIndex = "custom_findings_index_1",
                findingsIndexPattern = "custom_findings_index_1-1"
            )
        )

        val docLevelMonitorResponse1 = createMonitor(docLevelMonitor1)!!

        val queryMonitorInput = SearchInput(
            indices = listOf(index),
            query = SearchSourceBuilder().query(
                QueryBuilders
                    .rangeQuery("test_strict_date_time")
                    .gt("{{period_end}}||-10d")
                    .lte("{{period_end}}")
                    .format("epoch_millis")
            )
        )
        val queryTriggerScript = """
            return ctx.results[0].hits.hits.size() > 0
        """.trimIndent()

        val queryLevelTrigger = randomQueryLevelTrigger(condition = Script(queryTriggerScript))
        val queryMonitorResponse = createMonitor(randomQueryLevelMonitor(inputs = listOf(queryMonitorInput), triggers = listOf(queryLevelTrigger)))!!

        // 1. docMonitor (chainedFinding = null) 2. bucketMonitor (chainedFinding = docMonitor) 3. docMonitor (chainedFinding = bucketMonitor) 4. queryMonitor (chainedFinding = docMonitor 3)
        var workflow = randomWorkflowMonitor(
            monitorIds = listOf(docLevelMonitorResponse.id, bucketLevelMonitorResponse.id, docLevelMonitorResponse1.id, queryMonitorResponse.id)
        )
        val workflowResponse = upsertWorkflow(workflow)!!
        val workflowById = searchWorkflow(workflowResponse.id)!!
        assertNotNull(workflowById)

        // Creates 5 documents
        insertSampleTimeSerializedData(
            index,
            listOf(
                "test_value_1",
                "test_value_1", // adding duplicate to verify aggregation
                "test_value_2",
                "test_value_2",
                "test_value_3",
                "test_value_3"
            )
        )

        val workflowId = workflowResponse.id
        // 1. Doc level monitor should reduce the doc findings to 4 (3 - test_value_2, 4 - test_value_2, 5 - test_value_3, 6 - test_value_3)
        // 2. Bucket level monitor will match the fetch the docs from current findings execution, although it contains rules for matching documents which has test_value_2 and test value_3
        val executeWorkflowResponse = executeWorkflow(workflowById, workflowId, false)!!
        assertNotNull(executeWorkflowResponse)

        for (monitorRunResults in executeWorkflowResponse.workflowRunResult.workflowRunResult) {
            when (monitorRunResults.monitorName) {
                // Verify first doc level monitor execution, alerts and findings
                docLevelMonitorResponse.monitor.name -> {
                    assertEquals(1, monitorRunResults.inputResults.results.size)
                    val values = monitorRunResults.triggerResults.values
                    assertEquals(1, values.size)
                    @Suppress("UNCHECKED_CAST")
                    val docLevelTrigger = values.iterator().next() as DocumentLevelTriggerRunResult
                    val triggeredDocIds = docLevelTrigger.triggeredDocs.map { it.split("|")[0] }
                    val expectedTriggeredDocIds = listOf("3", "4", "5", "6")
                    assertEquals(expectedTriggeredDocIds, triggeredDocIds.sorted())

                    assertAlerts(docLevelMonitorResponse, docLevelMonitorResponse.monitor.dataSources.alertsIndex, 4)
                    assertFindings(docLevelMonitorResponse.id, docLevelMonitorResponse.monitor.dataSources.findingsIndex, 4, 4, listOf("3", "4", "5", "6"))
                }
                // Verify second bucket level monitor execution, alerts and findings
                bucketLevelMonitorResponse.monitor.name -> {
                    val searchResult = monitorRunResults.inputResults.results.first()
                    @Suppress("UNCHECKED_CAST")
                    val buckets = searchResult.stringMap("aggregations")?.stringMap("composite_agg")?.get("buckets") as List<Map<String, Any>>
                    assertEquals("Incorrect search result", 2, buckets.size)

                    assertAlerts(bucketLevelMonitorResponse, bucketLevelMonitorResponse.monitor.dataSources.alertsIndex, 2)
                    assertFindings(bucketLevelMonitorResponse.id, bucketLevelMonitorResponse.monitor.dataSources.findingsIndex, 1, 4, listOf("3", "4", "5", "6"))
                }
                // Verify third doc level monitor execution, alerts and findings
                docLevelMonitorResponse1.monitor.name -> {
                    assertEquals(1, monitorRunResults.inputResults.results.size)
                    val values = monitorRunResults.triggerResults.values
                    assertEquals(1, values.size)
                    @Suppress("UNCHECKED_CAST")
                    val docLevelTrigger = values.iterator().next() as DocumentLevelTriggerRunResult
                    val triggeredDocIds = docLevelTrigger.triggeredDocs.map { it.split("|")[0] }
                    val expectedTriggeredDocIds = listOf("5", "6")
                    assertEquals(expectedTriggeredDocIds, triggeredDocIds.sorted())

                    assertAlerts(docLevelMonitorResponse1, docLevelMonitorResponse1.monitor.dataSources.alertsIndex, 2)
                    assertFindings(docLevelMonitorResponse1.id, docLevelMonitorResponse1.monitor.dataSources.findingsIndex, 2, 2, listOf("5", "6"))
                }
                // Verify fourth query level monitor execution
                queryMonitorResponse.monitor.name -> {
                    assertEquals(1, monitorRunResults.inputResults.results.size)
                    val values = monitorRunResults.triggerResults.values
                    assertEquals(1, values.size)
                    @Suppress("UNCHECKED_CAST")
                    val totalHits = ((monitorRunResults.inputResults.results[0]["hits"] as Map<String, Any>)["total"] as Map<String, Any>) ["value"]
                    assertEquals(2, totalHits)
                    @Suppress("UNCHECKED_CAST")
                    val docIds = ((monitorRunResults.inputResults.results[0]["hits"] as Map<String, Any>)["hits"] as List<Map<String, String>>).map { it["_id"]!! }
                    assertEquals(listOf("5", "6"), docIds.sorted())
                }
            }
        }
    }

    fun `test execute workflow inout error`() {
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3"))
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)

        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )

        val monitorResponse = createMonitor(monitor)!!
        var workflow = randomWorkflowMonitor(
            monitorIds = listOf(monitorResponse.id)
        )
        val workflowResponse = upsertWorkflow(workflow)!!
        val workflowById = searchWorkflow(workflowResponse.id)!!
        assertNotNull(workflowById)

        deleteIndex(index)

        val response = executeWorkflow(workflowById, workflowById.id, false)!!

        assertNotNull(response.workflowRunResult.error)
        assertTrue(response.workflowRunResult.error is AlertingException)
        assertEquals(RestStatus.NOT_FOUND, (response.workflowRunResult.error as AlertingException).status)
        assertEquals("Configured indices are not found: [$index]", (response.workflowRunResult.error as AlertingException).message)
    }

    fun `test execute workflow wrong workflow id`() {
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3"))
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)

        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )

        val monitorResponse = createMonitor(monitor)!!

        val workflowRequest = randomWorkflowMonitor(
            monitorIds = listOf(monitorResponse.id)
        )
        val workflowResponse = upsertWorkflow(workflowRequest)!!
        val workflowId = workflowResponse.id
        val getWorkflowResponse = getWorkflowById(id = workflowResponse.id)

        assertNotNull(getWorkflowResponse)
        assertEquals(workflowId, getWorkflowResponse.id)

        var exception: Exception? = null
        val badWorkflowId = getWorkflowResponse.id + "bad"
        try {
            executeWorkflow(id = badWorkflowId)
        } catch (ex: Exception) {
            exception = ex
        }
        assertTrue(exception is ExecutionException)
        assertTrue(exception!!.cause is AlertingException)
        assertEquals(RestStatus.NOT_FOUND, (exception.cause as AlertingException).status)
        assertEquals("Can't find workflow with id: $badWorkflowId", exception.cause!!.message)
    }

    private fun assertFindings(
        monitorId: String,
        customFindingsIndex: String,
        findingSize: Int,
        matchedQueryNumber: Int,
        relatedDocIds: List<String>
    ) {
        val findings = searchFindings(monitorId, customFindingsIndex)
        assertEquals("Findings saved for test monitor", findingSize, findings.size)

        val findingDocIds = findings.flatMap { it.relatedDocIds }

        assertEquals("Didn't match $matchedQueryNumber query", matchedQueryNumber, findingDocIds.size)
        assertTrue("Findings saved for test monitor", relatedDocIds.containsAll(findingDocIds))
    }

    private fun assertAlerts(
        monitorResponse: IndexMonitorResponse,
        customAlertsIndex: String,
        alertSize: Int
    ) {
        val monitorId = monitorResponse.id
        val alerts = searchAlerts(monitorId, customAlertsIndex)
        assertEquals("Alert saved for test monitor", alertSize, alerts.size)
        val table = Table("asc", "id", null, alertSize, 0, "")
        var getAlertsResponse = client()
            .execute(
                AlertingActions.GET_ALERTS_ACTION_TYPE,
                GetAlertsRequest(table, "ALL", "ALL", null, customAlertsIndex)
            )
            .get()
        assertTrue(getAlertsResponse != null)
        assertTrue(getAlertsResponse.alerts.size == alertSize)
        getAlertsResponse = client()
            .execute(AlertingActions.GET_ALERTS_ACTION_TYPE, GetAlertsRequest(table, "ALL", "ALL", monitorId, null))
            .get()
        assertTrue(getAlertsResponse != null)
        assertTrue(getAlertsResponse.alerts.size == alertSize)

        val alertIds = getAlertsResponse.alerts.map { it.id }
        val acknowledgeAlertResponse = client().execute(
            AlertingActions.ACKNOWLEDGE_ALERTS_ACTION_TYPE,
            AcknowledgeAlertRequest(monitorId, alertIds, WriteRequest.RefreshPolicy.IMMEDIATE)
        ).get()

        assertEquals(alertSize, acknowledgeAlertResponse.acknowledged.size)
    }
}