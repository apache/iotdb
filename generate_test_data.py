from pathlib import Path


NUM_FIELDS = 50

CREATE_TABLE = (
    "CREATE TABLE IF NOT EXISTS t2 ("
    "device_id STRING TAG, "
    + ", ".join(f"s{i:02d} INT32 FIELD" for i in range(1, NUM_FIELDS + 1))
    + ");"
)


def build_sql_lines(start: int = 1, end: int = 1000, num_fields: int = NUM_FIELDS) -> list[str]:
    lines = ["use test;", CREATE_TABLE]
    for i in range(start, end + 1):
        field_values = ", ".join(str(i + j) for j in range(num_fields))
        lines.append(f"insert into t2 values(now(), 'd{i}', {field_values});")
    return lines


def build_heavy_sql_lines(
    num_devices: int = 50,
    rows_per_device: int = 2000,
    num_fields: int = NUM_FIELDS,
    base_ts: int = 1700000000000,
) -> list[str]:
    """Generate large volume data: num_devices × rows_per_device rows.
    Total rows = 50 × 2000 = 100,000 by default.
    """
    lines = ["use test;", CREATE_TABLE]
    for d in range(1, num_devices + 1):
        for r in range(rows_per_device):
            ts = base_ts + d * 100000 + r
            field_values = ", ".join(str((d + r + j) % 10000) for j in range(num_fields))
            lines.append(f"insert into t2 values({ts}, 'd{d}', {field_values});")
    return lines


def main() -> None:
    # Original 1K rows
    out_path = Path("insert_t_d1_d1000_50_fields.sql")
    sql = "\n".join(build_sql_lines()) + "\n"
    out_path.write_text(sql, encoding="utf-8")
    print(f"Generated: {out_path.resolve()}")

    # Heavy data: 100K rows for negative test case
    heavy_path = Path("insert_heavy_100k.sql")
    sql_heavy = "\n".join(build_heavy_sql_lines()) + "\n"
    heavy_path.write_text(sql_heavy, encoding="utf-8")
    print(f"Generated: {heavy_path.resolve()} ({len(sql_heavy) // 1024} KB)")


if __name__ == "__main__":
    main()
