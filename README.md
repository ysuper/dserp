DS ERP

config.py
=========
```python
from urllib.parse import quote_plus

db_cfg = dict(
    server="erp_db_ip",
    username="erp_db_username",
    password=quote_plus("erp_db_password"),
    db_name="erp_db_name",
)
```