select asset_id as Asset_ID
,client as Client
,solution as Solution
,to_char(due_date,'YYYY-MM-01') as monthdue
,site as Site

from public.req_corp_servers
