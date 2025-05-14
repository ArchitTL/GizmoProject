# --------------------------------------------
# GitHub Repo Configuration
# --------------------------------------------
github_repo_url = "https://github.com/Arch30it/CodeMigrationFramework.git"
github_branch_name = "main"


# --------------------------------------------
# Local Clone Paths and Log File Paths
# --------------------------------------------
clone_path = r"C:\Users\ArchitMurgudkar\Documents\TLConsulting\Projects\CodeMigrationWorkfkow\Star\github_clone"
log_migration_updates_file_path = r"C:\Users\ArchitMurgudkar\Documents\TLConsulting\Projects\CodeMigrationWorkfkow\Star\migration_log.jsonl"

# --------------------------------------------
# Default clone directory (used if clone_path not provided)
# --------------------------------------------
default_clone_path = "/tmp"

# --------------------------------------------
# File names for logging (auto-appended to path if log_path not provided)
# --------------------------------------------
log_code_flow_file_name = "code_debug.log"
log_migration_updates_file_name = "migration_log.jsonl"


# --------------------------------------------
#  File Extensions to Include/Exclude
# --------------------------------------------
inclusive_file_extensions = (".sql", ".py", ".ipynb", ".txt")
exclusive_file_extensions = ("",)  # currently unused but reserved


# --------------------------------------------
# Regex Patterns to Detect Legacy References
# --------------------------------------------
patterns = {
    "hive_metastore": r"hive_metastore\.\w+\.\w+",
    "adls_storage": r"abfss://[^\s\"']+",
    "wasbs_storage": r"wasbs://[^\s\"']+",
    "dbfs_storage": r"/dbfs/[^\s\"']+"
}


# --------------------------------------------
# CSV Paths for Table and Storage Mappings
# --------------------------------------------
table_csv_path = r"C:\Users\ArchitMurgudkar\Documents\TLConsulting\Projects\DemoProjects\MigrationDemo\ucx\configs\table_mappings.csv"
old_new_ref_csv_path = r"C:\Users\ArchitMurgudkar\Documents\TLConsulting\Projects\DemoProjects\MigrationDemo\ucx\configs\old__new_references.csv"


# --------------------------------------------
# CSV Column Headers
# --------------------------------------------
# For table_mappings.csv
# --------------------------------------------
col_hive_db = "hive_db"
col_hive_table = "hive_table"
col_uc_catalog = "uc_catalog"
col_uc_db = "hive_db"          # 👈 Note: Should this be 'uc_db'?
col_uc_table = "uc_table"

# --------------------------------------------
# For old__new_references.csv
# --------------------------------------------
col_old_path = "old"
col_new_path = "new"
