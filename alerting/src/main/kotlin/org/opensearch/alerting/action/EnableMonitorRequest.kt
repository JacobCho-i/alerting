package org.opensearch.alerting.action

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.support.WriteRequest
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.rest.RestRequest
import java.io.IOException

class EnableMonitorRequest : ActionRequest {
    val monitorId: String
    val seqNo: Long
    val refreshPolicy: WriteRequest.RefreshPolicy
    val enabled: Boolean

//    val primaryTerm: Long
//    val method: RestRequest.Method
//    var monitor: Monitor
//    val rbacRoles: List<String>?

    constructor(
        monitorId: String,
        seqNo: Long,
        refreshPolicy: WriteRequest.RefreshPolicy,
        enabled: Boolean,


//        primaryTerm: Long,
//        method: RestRequest.Method,
//        monitor: Monitor,
//        rbacRoles: List<String>? = null
    ) : super() {
        this.monitorId = monitorId
        this.seqNo = seqNo
        this.refreshPolicy = refreshPolicy
        this.enabled = enabled


//        this.primaryTerm = primaryTerm
//        this.method = method
//        this.monitor = monitor
//        this.rbacRoles = rbacRoles
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        monitorId = sin.readString(),
        seqNo = sin.readLong(),
        refreshPolicy = WriteRequest.RefreshPolicy.readFrom(sin),
        enabled = sin.readBoolean()
    )
//        primaryTerm = sin.readLong(),
//        method = sin.readEnum(RestRequest.Method::class.java),
//        monitor = Monitor.readFrom(sin) as Monitor,
//        rbacRoles = sin.readOptionalStringList()

    override fun validate(): ActionRequestValidationException? {
        return null
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(monitorId)
        out.writeLong(seqNo)
        refreshPolicy.writeTo(out)
        out.writeBoolean(enabled)

//        out.writeLong(primaryTerm)
//        out.writeEnum(method)
//        monitor.writeTo(out)
//        out.writeOptionalStringCollection(rbacRoles)
    }
}
