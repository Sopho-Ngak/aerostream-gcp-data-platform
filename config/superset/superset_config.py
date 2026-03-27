# Superset configuration
import os

# Database configuration
SQLALCHEMY_DATABASE_URI = os.getenv('SUPERSET__SQLALCHEMY_DATABASE_URI', 
                                    'postgresql://superset:superset@superset-db:5432/superset')

# Security
SECRET_KEY = os.getenv('SUPERSET_SECRET_KEY', 'your_secret_key_here_change_me_123456789')
WTF_CSRF_ENABLED = True
WTF_CSRF_TIME_LIMIT = None

# Feature flags
FEATURE_FLAGS = {
    'ENABLE_TEMPLATE_PROCESSING': True,
    'ENABLE_EXPLORE_JSON_CSRF_PROTECTION': False,
    'ENABLE_JAVASCRIPT_CONTROLS': True,
    'DASHBOARD_RBAC': True,
    'ENABLE_ADVANCED_DATA_TYPES': True,
    'ENABLE_EXPORT_API': True,
    'ENABLE_QUERY_HISTORY': True,
    'ALERT_REPORTS': True,
    'EMBEDDED_SUPERSET': True,
}

# Database settings
SQLALCHEMY_TRACK_MODIFICATIONS = False
SQLALCHEMY_ENGINE_OPTIONS = {
    'pool_size': 10,
    'pool_recycle': 3600,
    'pool_pre_ping': True,
}

# Cache settings
CACHE_CONFIG = {
    'CACHE_TYPE': 'SimpleCache',
    'CACHE_DEFAULT_TIMEOUT': 300,
}

# Dashboard settings
DASHBOARD_CACHE_TIMEOUT = 60
DASHBOARD_FILTERS_CACHE_TIMEOUT = 60

# Row level security
ROW_LEVEL_SECURITY = True

# UI settings
SUPERSET_WEBSERVER_PORT = 8088
SUPERSET_WEBSERVER_TIMEOUT = 60

# Allowed hosts
ALLOWED_HOSTS = ['*']

# CSV export
CSV_EXPORT = {
    'encoding': 'utf-8',
}

# Timezone
DEFAULT_TIMEZONE = 'UTC'

# BigQuery settings
BIGQUERY_CREDENTIALS_FILE = os.getenv('BIGQUERY_CREDENTIALS_FILE', '/app/bigquery-credentials.json')