-- Tickets per agent (Axpo Onsite Support CH - Baden) with monthly segmentation
-- Source: axsa_prod_bronze.jira_tickets_mtb.tickets
-- Notes:
--   - This SQL assumes canonical column names.
--   - The Python serving layer resolves schema variants at runtime.

SELECT
    DATE_FORMAT(created_in, 'yyyy-MM') AS month,
    COALESCE(TRIM(assignee), 'Unassigned') AS assignee,
    status,
    priority,
    COUNT(id) AS ticket_count,
    SUM(
        CASE
            WHEN LOWER(CAST(sla_breached AS STRING)) IN ('1', 'true', 'yes', 'y', 'breached', 'breach') THEN 1
            ELSE 0
        END
    ) AS sla_breach_count
FROM axsa_prod_bronze.jira_tickets_mtb.tickets
WHERE created_in IS NOT NULL
  AND project = 'IT Hub'
  AND assigned_group = 'Axpo Onsite Support CH - Baden'
  AND LOWER(type) IN (
    'system incident',
    'system service request',
    'system service request with approvals'
  )
GROUP BY
    DATE_FORMAT(created_in, 'yyyy-MM'),
    COALESCE(TRIM(assignee), 'Unassigned'),
    status,
    priority
ORDER BY month DESC, assignee ASC, status ASC, priority ASC;
