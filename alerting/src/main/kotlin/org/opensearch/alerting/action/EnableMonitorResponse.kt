package org.opensearch.alerting.action

import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.util.IndexUtils.Companion._ID
import org.opensearch.commons.alerting.util.IndexUtils.Companion._PRIMARY_TERM
import org.opensearch.commons.alerting.util.IndexUtils.Companion._SEQ_NO
import org.opensearch.commons.alerting.util.IndexUtils.Companion._VERSION
import org.opensearch.commons.notifications.action.BaseResponse
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentBuilder
import java.io.IOException

class EnableMonitorResponse : BaseResponse {
    var id: String
    var version: Long
    var seqNo: Long
    var enabled: Boolean
//    var primaryTerm: Long
//    var monitor: Monitor

    constructor(
        id: String,
        version: Long,
        seqNo: Long,
        enabled: Boolean,
//        primaryTerm: Long,
//        monitor: Monitor
    ) : super() {
        this.id = id
        this.version = version
        this.seqNo = seqNo
        this.enabled = enabled
//        this.primaryTerm = primaryTerm
//        this.monitor = monitor
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        sin.readString(), // id
        sin.readLong(), // version
        sin.readLong(), // seqNo
        sin.readBoolean(), // enabled
//        sin.readLong(), // primaryTerm
//        Monitor.readFrom(sin) as Monitor // monitor
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(id)
        out.writeLong(version)
        out.writeLong(seqNo)
        out.writeBoolean(enabled)
//        out.writeLong(primaryTerm)
//        monitor.writeTo(out)
    }

    @Throws(IOException::class)
    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .field(_ID, id)
            .field(_VERSION, version)
            .field(_SEQ_NO, seqNo)
            .field("enabled", enabled)
//            .field(_PRIMARY_TERM, primaryTerm)
//            .field("monitor", monitor)
            .endObject()
    }
}
