package org.opensearch.alerting.action

import org.opensearch.action.ActionType

class EnableMonitorAction private constructor() : ActionType<EnableMonitorResponse>(EnableMonitorAction.NAME, ::EnableMonitorResponse)  {
    companion object {
        val INSTANCE = EnableMonitorAction()
        const val NAME = "cluster:admin/opendistro/alerting/monitor/status"
    }
}
