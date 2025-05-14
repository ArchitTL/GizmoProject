import pandas as pd
from variables.variables import col_hive_db, col_hive_table, col_uc_catalog, col_uc_db, col_uc_table, col_old_path, col_new_path


def build_uc_mapping_from_csvs(table_csv_path: str, ref_csv_path: str) -> dict:
    """
    @Purpose:
    Combines two CSV files — one with Hive → UC table mappings, another with storage path rewrites —
    into a unified mapping dictionary to be consumed by the migration framework.
    """
    mapping = {}

    # Load Hive to Unity Catalog table mappings
    table_df = pd.read_csv(table_csv_path)
    for _, row in table_df.iterrows():
        hive_fqn = f"hive_metastore.{row[col_hive_db]}.{row[col_hive_table]}"
        uc_fqn = f"{row[col_uc_catalog]}.{row[col_uc_db]}.{row[col_uc_table]}"
        mapping[hive_fqn] = uc_fqn

    # Load ADLS/WASBS/DBFS path replacements
    ref_df = pd.read_csv(ref_csv_path)
    for _, row in ref_df.iterrows():
        mapping[row[col_old_path]] = row[col_new_path]

    return mapping
