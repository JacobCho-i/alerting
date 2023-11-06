package org.opensearch.alerting.action

import org.opensearch.action.ActionType

class EnableMonitorAction private constructor() : ActionType<ExecuteWorkflowResponse>(ExecuteWorkflowAction.NAME, ::ExecuteWorkflowResponse)  {
    companion object {
        val INSTANCE = EnableMonitorAction()
        const val NAME = "cluster:admin/opendistro/alerting/monitor/enable"
    }
}
