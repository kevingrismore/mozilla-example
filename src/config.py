PROJECT_ID = "prefect-sbx-sales-engineering"
EXPORT_DATASET_ID = "apple_app_store_exported"
DERIVED_DATASET_ID = "apple_app_store"

APPS = [
    ("989804926", "Firefox"),
    ("1489407738", "VPN"),
    ("1295998056", "WebXRViewer"),
    ("1314000270", "Lockwise"),
    ("1073435754", "Klar"),
    ("1055677337", "Focus"),
]

DERIVED_TABLES = [
    "metrics_by_app_referrer",
    "metrics_by_app_version",
    "metrics_by_campaign",
    "metrics_by_platform",
    "metrics_by_platform_version",
    "metrics_by_region",
    "metrics_by_source",
    "metrics_by_storefront",
    "metrics_by_web_referrer",
    "metrics_total",
]
