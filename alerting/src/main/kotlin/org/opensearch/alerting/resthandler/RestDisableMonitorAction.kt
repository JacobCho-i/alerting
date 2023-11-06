package org.opensearch.alerting.resthandler

import org.apache.logging.log4j.LogManager
import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.action.*
import org.opensearch.alerting.alerts.AlertIndices
import org.opensearch.alerting.util.IF_PRIMARY_TERM
import org.opensearch.alerting.util.IF_SEQ_NO
import org.opensearch.alerting.util.REFRESH
import org.opensearch.client.node.NodeClient
import org.opensearch.commons.alerting.model.BucketLevelTrigger
import org.opensearch.commons.alerting.model.DocumentLevelTrigger
import org.opensearch.commons.alerting.model.QueryLevelTrigger
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParserUtils
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.rest.*
import org.opensearch.rest.action.RestResponseListener
import java.io.IOException
import java.time.Instant

/**
 * Rest handlers to disable monitor
 */
class RestDisableMonitorAction: BaseRestHandler() {

    private val log = LogManager.getLogger(javaClass)
    override fun getName(): String {
        return "disable_monitor"
    }

    override fun routes(): List<RestHandler.Route> {
        return listOf(
            RestHandler.Route(
                RestRequest.Method.PUT,
                "_plugins/_alerting/monitors/{monitor_id}/disable"
            )
        )
    }
    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        log.debug("${request.method()} ${AlertingPlugin.MONITOR_BASE_URI}")

        val id = request.param("monitorID", org.opensearch.commons.alerting.model.Monitor.NO_ID)
        if (request.method() == RestRequest.Method.PUT && org.opensearch.commons.alerting.model.Monitor.NO_ID == id) {
            throw IllegalArgumentException("Missing monitor ID")
        }

        // Validate request by parsing JSON to Monitor
        val xcp = request.contentParser()
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
        val monitor = org.opensearch.commons.alerting.model.Monitor.parse(xcp, id).copy(lastUpdateTime = Instant.now())
        val rbacRoles = request.contentParser().map()["rbac_roles"] as List<String>?

        validateDataSources(monitor)
        val monitorType = monitor.monitorType
        val triggers = monitor.triggers
        when (monitorType) {
            org.opensearch.commons.alerting.model.Monitor.MonitorType.QUERY_LEVEL_MONITOR -> {
                triggers.forEach {
                    if (it !is QueryLevelTrigger) {
                        throw IllegalArgumentException("Illegal trigger type, ${it.javaClass.name}, for query level monitor")
                    }
                }
            }
            org.opensearch.commons.alerting.model.Monitor.MonitorType.BUCKET_LEVEL_MONITOR -> {
                triggers.forEach {
                    if (it !is BucketLevelTrigger) {
                        throw IllegalArgumentException("Illegal trigger type, ${it.javaClass.name}, for bucket level monitor")
                    }
                }
            }
            org.opensearch.commons.alerting.model.Monitor.MonitorType.CLUSTER_METRICS_MONITOR -> {
                triggers.forEach {
                    if (it !is QueryLevelTrigger) {
                        throw IllegalArgumentException("Illegal trigger type, ${it.javaClass.name}, for cluster metrics monitor")
                    }
                }
            }
            org.opensearch.commons.alerting.model.Monitor.MonitorType.DOC_LEVEL_MONITOR -> {
                triggers.forEach {
                    if (it !is DocumentLevelTrigger) {
                        throw IllegalArgumentException("Illegal trigger type, ${it.javaClass.name}, for document level monitor")
                    }
                }
            }
        }
        val seqNo = request.paramAsLong(IF_SEQ_NO, SequenceNumbers.UNASSIGNED_SEQ_NO)
        val primaryTerm = request.paramAsLong(IF_PRIMARY_TERM, SequenceNumbers.UNASSIGNED_PRIMARY_TERM)
        val refreshPolicy = if (request.hasParam(REFRESH)) {
            WriteRequest.RefreshPolicy.parse(request.param(REFRESH))
        } else {
            WriteRequest.RefreshPolicy.IMMEDIATE
        }
        val disableMonitorRequest = DisableMonitorRequest(id, seqNo, primaryTerm, refreshPolicy, request.method(), monitor, rbacRoles)

        return RestChannelConsumer { channel ->
            client.execute(DisableMonitorAction.INSTANCE, disableMonitorRequest, disableMonitorResponse(channel, request.method()))
        }
    }

    private fun validateDataSources(monitor: org.opensearch.commons.alerting.model.Monitor) { // Data Sources will currently be supported only at transport layer.
        if (monitor.dataSources != null) {
            if (
                monitor.dataSources.queryIndex != ScheduledJob.DOC_LEVEL_QUERIES_INDEX ||
                monitor.dataSources.findingsIndex != AlertIndices.FINDING_HISTORY_WRITE_INDEX ||
                monitor.dataSources.alertsIndex != AlertIndices.ALERT_INDEX
            ) {
                throw IllegalArgumentException("Custom Data Sources are not allowed.")
            }
        }
    }

    private fun disableMonitorResponse(channel: RestChannel, restMethod: RestRequest.Method):
            RestResponseListener<DisableMonitorResponse> {
        return object : RestResponseListener<DisableMonitorResponse>(channel) {
            @Throws(Exception::class)
            override fun buildResponse(response: DisableMonitorResponse): RestResponse {
                var returnStatus = RestStatus.CREATED
                if (restMethod == RestRequest.Method.PUT)
                    returnStatus = RestStatus.OK

                val restResponse = BytesRestResponse(returnStatus, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS))
                if (returnStatus == RestStatus.CREATED) {
                    val location = "${AlertingPlugin.MONITOR_BASE_URI}/${response.id}"
                    restResponse.addHeader("Location", location)
                }
                return restResponse
            }
        }
    }
}
