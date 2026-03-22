import os, json

#1/13 optional backends
POLARS_OK=True
DUCKDB_OK=True
PYARROW_OK=True
try:
    import polars as pl
except Exception:
    POLARS_OK=False
try:
    import duckdb
except Exception:
    DUCKDB_OK=False
try:
    import pyarrow
except Exception:
    PYARROW_OK=False

#2/13 foundation
def build_performance_foundation(project_root, raw_root, time_registry_path):
    registry={}
    if os.path.exists(time_registry_path):
        try:
            with open(time_registry_path,"r",encoding="utf-8") as f:
                registry=json.load(f)
        except Exception:
            registry={}

    dataset_count=0
    path_index=registry.get("path_index",{}) if isinstance(registry,dict) else {}
    datasets=[]
    for p,meta in path_index.items():
        dataset_count+=1
        datasets.append({
            "path":p,
            "time_col":meta.get("time_col"),
            "time_format":meta.get("time_format"),
            "has_ohlc":meta.get("has_ohlc"),
            "open_col":meta.get("open_col"),
            "close_col":meta.get("close_col"),
        })

    return {
        "project_root":project_root,
        "raw_root":raw_root,
        "polars_available":POLARS_OK,
        "duckdb_available":DUCKDB_OK,
        "pyarrow_available":PYARROW_OK,
        "dataset_count_from_time_registry":dataset_count,
        "time_registry_based_datasets":datasets[:50],
        "recommended_scan_stack":[
            "polars_lazy_for_csv_scans",
            "duckdb_for_wide_cross_asset_joins",
            "time_registry_first_parsing",
            "ex_ante_join_layer_before_business_logic"
        ],
        "next_perf_targets":[
            "cross asset heavy scans",
            "calendar x asset x asset joins",
            "three-way mixed cases",
            "opening-window scans with strict ex-ante filtering"
        ]
    }
