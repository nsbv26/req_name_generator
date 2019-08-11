select a.id as Asset_ID
,f.mnemonic as Client
,c.name as Solution
,to_char(a.due_date,'YYYY-MM-01') as monthdue
,d.name as Site

from assets a
JOIN compute_configs b ON b.id = a.compute_config_id
JOIN solutions c ON c.id = a.solution_id
JOIN sites d ON d.id = a.site_id
JOIN requests e ON e.id = a.request_id
JOIN clients f ON f.id = e.client_id
JOIN requests_users u ON u.request_id = a.request_id
JOIN users ud ON ud.id = u.user_id
WHERE a.status=(9)
