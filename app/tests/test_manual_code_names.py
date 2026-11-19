"""The hand-curated code list fills in missing species names, and nothing else."""

from __future__ import annotations

import duckdb
import pytest

from app.src.convert_mdb_to_duckdb import fill_missing_names_from_manual_list


@pytest.fixture
def con():
    c = duckdb.connect(":memory:")
    for table in ("ringon", "kontr", "fynd", "frring"):
        c.execute(f"CREATE TABLE {table} (species_code VARCHAR)")
    c.execute("CREATE TABLE artkod_lookup (artkod VARCHAR, swedish_name VARCHAR)")
    c.execute("CREATE TABLE species_metadata (swedish_name VARCHAR PRIMARY KEY, scientific_name VARCHAR, english_name VARCHAR)")
    c.execute("INSERT INTO artkod_lookup VALUES ('RÖHAK', 'rödhake'), ('KÄSNÄ', 'kärrsnäppa')")
    c.execute("INSERT INTO species_metadata VALUES ('rödhake', 'Erithacus rubecula', 'European Robin')")
    # RÖHAK and KÄSNÄ are named; SPGÅS, ABCDE and ZZZZZ are not; SPGÅS only occurs as a find.
    c.execute("INSERT INTO ringon VALUES ('RÖHAK'), ('RÖHAK'), ('ABCDE'), ('ZZZZZ')")
    c.execute("INSERT INTO kontr VALUES ('KÄSNÄ'), ('ZZZZZ')")
    c.execute("INSERT INTO fynd VALUES ('SPGÅS')")
    c.execute("INSERT INTO frring VALUES (NULL), ('')")
    return c


def write(tmp_path, rows):
    path = tmp_path / "manual.csv"
    # UTF-8 with a BOM, as Excel writes it.
    path.write_text("Artkod,Svenskt namn,Latinskt namn\n" + "\n".join(rows) + "\n", encoding="utf-8-sig")
    return path


def names(con):
    return dict(con.execute("SELECT artkod, swedish_name FROM artkod_lookup").fetchall())


def test_names_only_the_codes_that_are_missing(con, tmp_path):
    csv = write(tmp_path, [
        "SPGÅS,Spetsbergsgås,Anser brachyrhynchus",
        "ABCDE,Gråsiska (cabaret),Acanthis cabaret",
        "XXXXX,Aldrig sedd,Nulla nulla",            # not in the data: not added
        "RÖHAK,Annan rödhake,Erithacus x",          # already named: keeps its name
        "ZZZZZ,,Sine nomine",                       # no Swedish name: stays missing
    ])
    fill_missing_names_from_manual_list(con, csv)
    got = names(con)
    assert got["SPGÅS"] == "spetsbergsgås"                      # lower case, found only in a find table
    assert got["ABCDE"] == "gråsiska (cabaret)"
    assert got["RÖHAK"] == "rödhake" and got["KÄSNÄ"] == "kärrsnäppa"
    assert "XXXXX" not in got and "ZZZZZ" not in got
    assert len(got) == 4


def test_latin_names_are_added_without_touching_existing_taxonomy(con, tmp_path):
    csv = write(tmp_path, ["SPGÅS,Spetsbergsgås,Anser brachyrhynchus", "RÖHAK,Rödhake,Wrong name"])
    fill_missing_names_from_manual_list(con, csv)
    rows = dict(con.execute("SELECT swedish_name, scientific_name FROM species_metadata").fetchall())
    assert rows["spetsbergsgås"] == "Anser brachyrhynchus"
    assert rows["rödhake"] == "Erithacus rubecula"             # untouched


def test_running_twice_changes_nothing_more(con, tmp_path):
    csv = write(tmp_path, ["SPGÅS,Spetsbergsgås,Anser brachyrhynchus"])
    fill_missing_names_from_manual_list(con, csv)
    fill_missing_names_from_manual_list(con, csv)
    assert con.execute("SELECT count(*) FROM artkod_lookup WHERE artkod = 'SPGÅS'").fetchone()[0] == 1
    assert con.execute("SELECT count(*) FROM species_metadata WHERE swedish_name = 'spetsbergsgås'").fetchone()[0] == 1


def test_a_missing_file_is_skipped(con, tmp_path, capsys):
    fill_missing_names_from_manual_list(con, tmp_path / "nope.csv")
    assert "not found" in capsys.readouterr().out
    assert len(names(con)) == 2
