# Document-Level Alerting RFC

The purpose of this request for comments (RFC) is to introduce our plans to enhance the Alerting plugin with document-level alerting functionality and collect feedback and discuss our plans with the community. This RFC is meant to cover the high-level functionality and does not go into implementation details and architecture.

## Problem Statement

Currently, the Alerting plugin does not support a simple way to create Monitors that alert on each document in the index. As well as, there is currently no support to ensure there is no gap or overlap in data being monitored.

A common use case for alerting on a single document would be for monitoring an index that is ingesting audit log data for a set of servers. In this case, a monitor would be to setup an alert when the IP address in the audit log indicates an IP address that is not allowed to access the servers. The expectation here would be that each time the alert is generated, the user is notified of the problem and they can then view the exact record to investigate the security breach. 

This use case can somewhat be accomplished today through a Query-level or Bucket-level monitors with a query that searches for security breaches in the audit logs. Then the trigger can generate an alert when there are any matches for security breaches and generate an alert. However, the alerts will only give a summarized view of the security breaches and the user is not given the exact records that caused the issue, so they would need to dive into the data to even find the records that caused the security breaches. 

## Proposed Solution

The proposed solution is to offer more granular alerting by allowing alerts to be created based on each document in the index. This means that there could be multiple alerts even for a single trigger condition, ensuring that every event is captured.  

### Monitoring new data 
As we are alerting on each document for Document Level Alerting, we need to ensure each monitor execution only looks at new data as there will then be duplicate alerts. Additionally, between each monitor execution, there can be no gaps in data being monitored as then that would defeat the purpose of this monitoring solution. By supporting this, the monitor executions would no longer be looking at a time range of data, which most query-level and bucket-level monitors do. 

## Providing Feedback

If you have comments or feedback on our plans for Document-Level Alerting, please comment on the [GitHub issue](https://github.com/opensearch-project/alerting/issues/238) in this project to discuss.
