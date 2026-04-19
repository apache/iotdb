from pathlib import Path


def build_sql_lines(start: int = 1, end: int = 1000) -> list[str]:
    lines = ["use test;"]
    for i in range(start, end + 1):
        lines.append(f"insert into t values(now(), 'd{i}', {i});")
    return lines


def main() -> None:
    out_path = Path("insert_t_d1_d1000.sql")
    sql = "\n".join(build_sql_lines()) + "\n"
    out_path.write_text(sql, encoding="utf-8")
    print(f"Generated SQL file: {out_path.resolve()}")


if __name__ == "__main__":
    main()
