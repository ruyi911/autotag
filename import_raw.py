from pathlib import Path
import duckdb

PROJECT = Path.home() / "Desktop" / "autotag"
CSV_ROOT = PROJECT / "data" / "csv"
DB_PATH = PROJECT / "data" / "serving.duckdb"
DATASETS = {
     "充值订单": "recharge_orders",
     "提现订单": "withdraw_orders",
     "投注数据": "bets",
     "彩金数据": "bonuses",
     "用户数据": "users",
 }
#DATASETS = {
#    "投注数据": "bets"
#}

def import_one_folder(con, folder_name: str, table_name: str):
    folder = CSV_ROOT / folder_name
    if not folder.exists():
        raise FileNotFoundError(f"找不到目录: {folder}")

    csv_files = sorted(folder.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"{folder} 里没有 csv")

    glob_path = (folder / "*.csv").as_posix()
    print(f"\n==> Import {folder_name} ({len(csv_files)} files) -> raw.{table_name}")

    con.execute(f"""
        CREATE OR REPLACE TABLE raw.{table_name} AS
        SELECT *
        FROM read_csv_auto(
    '{glob_path}',
    union_by_name=true,
    normalize_names=false,
    strict_mode=false,
    ignore_errors=true
);
    """)

    rows = con.execute(f"SELECT COUNT(*) FROM raw.{table_name};").fetchone()[0]
    cols = con.execute(f"""
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_schema='raw' AND table_name='{table_name}';
    """).fetchone()[0]
    print(f"   rows={rows}, cols={cols}")

def main():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(DB_PATH.as_posix())
    con.execute("CREATE SCHEMA IF NOT EXISTS raw;")

    for folder_name, table_name in DATASETS.items():
        import_one_folder(con, folder_name, table_name)

    print("\n==> Done. Imported tables:")
    for (t,) in con.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema='raw'
        ORDER BY table_name;
    """).fetchall():
        print(" - raw." + t)

    con.close()
    print("\nDB:", DB_PATH)

if __name__ == "__main__":
    main()