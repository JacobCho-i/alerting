package org.opensearch.alerting.action

import org.opensearch.action.ActionType

class DisableMonitorAction private constructor() : ActionType<ExecuteWorkflowResponse>(ExecuteWorkflowAction.NAME, ::ExecuteWorkflowResponse)  {

    companion object {
        val INSTANCE = DisableMonitorAction()
        const val NAME = "cluster:admin/opendistro/alerting/monitor/disable"
    }

}
