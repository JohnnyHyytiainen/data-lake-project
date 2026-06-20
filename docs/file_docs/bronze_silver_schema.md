# Docs regarding my BRONZE SCHEMA + SILVER SCHEMA

## Bronze schema:

- Antal columns i bronze är: 10


|column_name | column_type | null | key | default | extra|
|:---|:---|:---|:---|:---|:---|
|id|	VARCHAR|	YES|	None|	None|	None|
|type|	VARCHAR|	YES|	None|	None|	None|
|actor|	STRUCT(avatar_url VARCHAR, display_login VARCHAR, gravatar_id VARCHAR, id BIGINT, login VARCHAR,...|	YES|	None|	None|	None|
|repo|	STRUCT(id BIGINT, "name" VARCHAR, url VARCHAR)|	YES|	None|	None|	None|
|payload|	VARCHAR|	YES|	None|	None|	None|
|public|	BOOLEAN|	YES|	None|	None|	None|
|created_at|	VARCHAR|	YES|	None|	None|	None|
|day|	VARCHAR|	YES|	None|	None|	None|
|month|	VARCHAR|	YES|	None|	None|	None|
|year|	BIGINT|	YES|	None|	None|	None|




## Silver schema:
- antal columns i silver är: 14

| column_name | column_type | null | key | default | extra |
|:---|:---|:---|:---|:---|:---|
| event_id	|VARCHAR|	YES|	None|	None|	None|
| event_type|	VARCHAR|	YES|	None|	None|	None|
| actor_login|	VARCHAR|	YES|	None|	None|	None|
| repo_name|	VARCHAR|	YES|	None|	None|	None|
| repo_id|	VARCHAR|	YES|	None|	None|	None|
| commit_count|	INTEGER|	YES|	None|	None|	None|
| pr_number|	INTEGER|	YES|	None|	None|	None|
| event_action|	VARCHAR|	YES|	None|	None|	None|
| pr_merged|	BOOLEAN|	YES|	None|	None|	None|
| created_at|	TIMESTAMP|	YES|	None|	None|	None|
| is_bot|	BOOLEAN|	YES|	None|	None|	None | 
| day|	VARCHAR|	YES|	None|	None|	None|
| month|	VARCHAR	|YES	|None	|None	|None|
| year|	BIGINT|	YES|	None|	None|	None|
