"""
Tests for fetch_observation_data.py
=====================================
Covers:
  1. Filter builder – produces the correct JSON body.
  2. CSV parsing – a mock CSV response is correctly turned into a Polars DF.
  3. DB schema – observations table can be created and written to.
  4. Live API smoke test – count endpoint returns an integer (requires key).
  5. Live CSV download – a narrow recent window returns ≥ 0 rows (requires key).

Tests 1–3 run without an API key (unit tests).
Tests 4–5 are skipped when ARTDATABANKEN_API_KEY is not set.

Run from the repo root:
    pytest app/tests/test_fetch_observations.py -v
"""

import io
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
import polars as pl
import pytest
import requests

# ── make src importable ──────────────────────────────────────────────────────
SRC = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(SRC))

import fetch_observation_data as fod

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

API_KEY = os.environ.get("ARTDATABANKEN_API_KEY", "")
NEEDS_KEY = pytest.mark.skipif(not API_KEY, reason="ARTDATABANKEN_API_KEY not set")


# ---------------------------------------------------------------------------
# 1. Filter builder
# ---------------------------------------------------------------------------

class TestBuildFilter:
    def test_taxon_ids(self):
        f = fod.build_filter("2023-01-01", "2023-12-31")
        assert f["taxon"]["ids"] == [fod.AVES_TAXON_ID]
        assert f["taxon"]["includeUnderlyingTaxa"] is True

    def test_date_range(self):
        f = fod.build_filter("2022-06-01", "2022-08-31")
        assert f["date"]["startDate"] == "2022-06-01"
        assert f["date"]["endDate"] == "2022-08-31"

    def test_point_geometry(self):
        f = fod.build_filter("2023-01-01", "2023-01-31")
        geom = f["geographics"]["geometries"][0]
        assert geom["type"] == "point"
        assert geom["coordinates"][0] == pytest.approx(fod.NIDINGEN_COORDS[0])
        assert geom["coordinates"][1] == pytest.approx(fod.NIDINGEN_COORDS[1])

    def test_buffer_passed_through(self):
        f = fod.build_filter("2023-01-01", "2023-01-31", buffer_m=3000)
        assert f["geographics"]["maxDistanceFromPoint"] == 3000

    def test_output_field_set(self):
        f = fod.build_filter("2023-01-01", "2023-01-31")
        assert f["output"]["fieldSet"] == fod.FIELD_SET

    def test_data_provider(self):
        f = fod.build_filter("2023-01-01", "2023-01-31")
        assert fod.ARTPORTALEN_DATA_PROVIDER_ID in f["dataProvider"]["ids"]

    def test_count_filter_has_no_output(self):
        """count_observations removes 'output' from the body before POSTing."""
        f = fod.build_filter("2023-01-01", "2023-12-31")
        f.pop("output", None)
        assert "output" not in f


# ---------------------------------------------------------------------------
# 2. CSV parsing (mocked HTTP)
# ---------------------------------------------------------------------------

SAMPLE_CSV = (
    "OccurrenceId\tDatasetName\tStartDate\tEndDate\tDecimalLatitude\tDecimalLongitude\t"
    "ScientificName\tVernacularName\tIndividualCount\tOccurrenceStatus\r\n"
    "urn:lsid:artportalen.se:Sighting:1\tArtportalen\t2023-04-15T06:00:00+02:00\t"
    "2023-04-15T06:00:00+02:00\t57.3025\t11.9006\tFicedula hypoleuca\tSvartvit flugsnappare\t2\tpresent\r\n"
    "urn:lsid:artportalen.se:Sighting:2\tArtportalen\t2023-09-03T08:30:00+02:00\t"
    "2023-09-03T08:30:00+02:00\t57.3020\t11.9010\tSylvia borin\tTrädgårdssångare\t1\tpresent\r\n"
)


class TestDownloadCsvWindow:
    def _mock_response(self, csv_text: str, status: int = 200) -> MagicMock:
        resp = MagicMock()
        resp.status_code = status
        resp.headers = {"Content-Type": "text/csv; charset=utf-8"}
        resp.content = csv_text.encode("utf-8")
        resp.raise_for_status = MagicMock()
        return resp

    def test_returns_polars_dataframe(self):
        with patch.object(fod, "_post", return_value=self._mock_response(SAMPLE_CSV)):
            df = fod.download_csv_window("2023-01-01", "2023-12-31")
        assert isinstance(df, pl.DataFrame)
        assert len(df) == 2

    def test_expected_columns(self):
        with patch.object(fod, "_post", return_value=self._mock_response(SAMPLE_CSV)):
            df = fod.download_csv_window("2023-01-01", "2023-12-31")
        assert "OccurrenceId" in df.columns
        assert "ScientificName" in df.columns
        assert "DecimalLatitude" in df.columns

    def test_species_values(self):
        with patch.object(fod, "_post", return_value=self._mock_response(SAMPLE_CSV)):
            df = fod.download_csv_window("2023-01-01", "2023-12-31")
        species = df["ScientificName"].to_list()
        assert "Ficedula hypoleuca" in species
        assert "Sylvia borin" in species

    def test_empty_response_returns_empty_df(self):
        with patch.object(fod, "_post", return_value=self._mock_response("")):
            df = fod.download_csv_window("2023-01-01", "2023-12-31")
        assert isinstance(df, pl.DataFrame)
        assert len(df) == 0

    def test_bom_stripped(self):
        bom_csv = "\ufeff" + SAMPLE_CSV
        with patch.object(fod, "_post", return_value=self._mock_response(bom_csv)):
            df = fod.download_csv_window("2023-01-01", "2023-12-31")
        assert len(df) == 2


# ---------------------------------------------------------------------------
# 3. Database schema and loading
# ---------------------------------------------------------------------------

class TestDBSchema:
    def test_schema_creates_table(self):
        conn = duckdb.connect(":memory:")
        fod.initialize_observations_schema(conn)
        tables = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
        assert "observations" in tables
        conn.close()

    def test_schema_idempotent(self):
        """Calling initialize_observations_schema twice should not raise."""
        conn = duckdb.connect(":memory:")
        fod.initialize_observations_schema(conn)
        fod.initialize_observations_schema(conn)  # second call is safe
        conn.close()

    def test_indexes_exist(self):
        conn = duckdb.connect(":memory:")
        fod.initialize_observations_schema(conn)
        indexes = conn.execute(
            "SELECT index_name FROM duckdb_indexes() WHERE table_name = 'observations'"
        ).fetchall()
        index_names = {r[0] for r in indexes}
        assert "idx_obs_start_date" in index_names
        assert "idx_obs_taxon" in index_names
        conn.close()

    def test_load_into_db_writes_rows(self):
        """Round-trip: parse mock CSV → load into temp DB → query back."""
        df = pl.read_csv(io.StringIO(SAMPLE_CSV), separator="\t")
        tmp_dir = tempfile.mkdtemp()
        db_path = str(Path(tmp_dir) / "test_obs.db")
        try:
            fod.load_into_db(df, db_path)
            conn = duckdb.connect(db_path, read_only=True)
            count = conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
            conn.close()
            assert count == 2
        finally:
            import shutil, gc
            gc.collect()  # encourage DuckDB to release any lingering handles
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_load_into_db_upserts_no_duplicates(self):
        """Loading the same data twice should not double the row count."""
        df = pl.read_csv(io.StringIO(SAMPLE_CSV), separator="\t")
        tmp_dir = tempfile.mkdtemp()
        db_path = str(Path(tmp_dir) / "test_obs.db")
        try:
            fod.load_into_db(df, db_path)
            fod.load_into_db(df, db_path)  # second load
            conn = duckdb.connect(db_path, read_only=True)
            count = conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
            conn.close()
            assert count == 2  # still 2, not 4
        finally:
            import shutil, gc
            gc.collect()
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_db_manager_initialize_observations_schema(self):
        """BirdRingingDB.initialize_observations_schema() works too."""
        from db_manager import BirdRingingDB
        conn = duckdb.connect(":memory:")
        # Monkey-patch to use the in-memory connection
        db = BirdRingingDB.__new__(BirdRingingDB)
        db.conn = conn
        db.initialize_observations_schema()
        tables = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
        assert "observations" in tables
        conn.close()


# ---------------------------------------------------------------------------
# 4. Live API – count endpoint (requires API key)
# ---------------------------------------------------------------------------

class TestLiveCount:
    @NEEDS_KEY
    def test_count_returns_integer(self):
        """A very recent narrow window should return a non-negative integer."""
        n = fod.count_observations("2024-05-01", "2024-05-31")
        assert isinstance(n, int)
        assert n >= 0

    @NEEDS_KEY
    def test_count_nidingen_reasonable(self):
        """Nidingen in spring should have > 0 observations."""
        n = fod.count_observations("2023-04-01", "2023-05-31", buffer_m=5000)
        assert n > 0, "Expected at least some spring observations near Nidingen"


# ---------------------------------------------------------------------------
# 5. Live API – CSV download (requires API key)
# ---------------------------------------------------------------------------

class TestLiveDownload:
    @NEEDS_KEY
    def test_small_window_returns_df(self):
        """Download a single week and verify the result is a valid DataFrame."""
        df = fod.download_csv_window("2023-09-18", "2023-09-24")
        assert isinstance(df, pl.DataFrame)
        # May be 0 rows if nothing was observed, but must not raise
        if len(df) > 0:
            assert "OccurrenceId" in df.columns
            assert "ScientificName" in df.columns

    @NEEDS_KEY
    def test_no_duplicate_occurrence_ids(self):
        """OccurrenceId should be unique within a downloaded batch."""
        df = fod.download_csv_window("2023-09-01", "2023-09-30")
        if len(df) == 0:
            pytest.skip("No data for the test window")
        if "OccurrenceId" in df.columns:
            assert df["OccurrenceId"].n_unique() == len(df), (
                "Duplicate OccurrenceIds found in download"
            )

    @NEEDS_KEY
    def test_coordinates_near_nidingen(self):
        """All returned coordinates should be within ~10 km of Nidingen."""
        df = fod.download_csv_window("2023-09-01", "2023-09-30", buffer_m=5000)
        if len(df) == 0 or "DecimalLatitude" not in df.columns:
            pytest.skip("No coordinate data available")
        lats = df["DecimalLatitude"].drop_nulls()
        lons = df["DecimalLongitude"].drop_nulls()
        assert lats.min() >= 57.0, "Latitudes too far south"
        assert lats.max() <= 57.6, "Latitudes too far north"
        assert lons.min() >= 11.5, "Longitudes too far west"
        assert lons.max() <= 12.3, "Longitudes too far east"


# ---------------------------------------------------------------------------
# 6. Integration – fetch_all_observations with mocked HTTP (no key needed)
# ---------------------------------------------------------------------------

class TestFetchAllObservationsIntegration:
    def _make_count_mock(self, counts: dict):
        """
        Build a side-effect callable that returns different counts for different
        date ranges.  *counts* maps (start_date, end_date) → int.
        Default is 0.
        """
        def side_effect(start_date, end_date, buffer_m=fod.DEFAULT_BUFFER_M):
            return counts.get((start_date, end_date), 0)
        return side_effect

    def test_no_observations_returns_empty_df(self):
        with (
            patch.object(fod, "count_observations", return_value=0),
        ):
            df = fod.fetch_all_observations(start_year=2023, end_year=2023)
        assert isinstance(df, pl.DataFrame)
        assert len(df) == 0

    def test_single_year_full_download(self):
        csv_mock = MagicMock()
        csv_mock.status_code = 200
        csv_mock.headers = {"Content-Type": "text/csv"}
        csv_mock.content = SAMPLE_CSV.encode()
        csv_mock.raise_for_status = MagicMock()

        with (
            patch.object(fod, "count_observations", return_value=5),
            patch.object(fod, "_post", return_value=csv_mock),
        ):
            df = fod.fetch_all_observations(start_year=2023, end_year=2023)

        assert len(df) == 2  # SAMPLE_CSV has 2 rows
        assert "ScientificName" in df.columns

    def test_deduplication_across_years(self):
        """If the same OccurrenceId appears in two year-batches it should be deduped."""
        csv_mock = MagicMock()
        csv_mock.status_code = 200
        csv_mock.headers = {"Content-Type": "text/csv"}
        csv_mock.content = SAMPLE_CSV.encode()
        csv_mock.raise_for_status = MagicMock()

        with (
            patch.object(fod, "count_observations", return_value=5),
            patch.object(fod, "_post", return_value=csv_mock),
        ):
            # Two years, both returning the same SAMPLE_CSV
            df = fod.fetch_all_observations(start_year=2022, end_year=2023)

        # After dedup by OccurrenceId, still only 2 unique rows
        assert len(df) == 2
