"""
Test script to verify database and data processing setup.

Run this script after initializing the database to ensure everything works correctly.
"""

from pathlib import Path
import sys

# Add src to path
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

from db_manager import BirdRingingDB
from data_processor import BirdDataProcessor
from query_utils import BirdRingingQueries


def test_database():
    """Test database operations."""
    print("\n" + "="*60)
    print("Testing Database Operations")
    print("="*60)
    
    db_path = Path(__file__).parent / "data" / "bird_ringing.db"
    
    if not db_path.exists():
        print("❌ Database not found. Please run initialize_database.py first.")
        return False
    
    try:
        with BirdRingingDB(db_path, read_only=True) as db:
            # Test 1: Get summary stats
            print("\n✓ Test 1: Getting summary statistics...")
            stats = db.get_summary_stats()
            print(f"  - Total records: {stats['total_records']:,}")
            print(f"  - Unique species: {stats['unique_species']}")
            
            # Test 2: Query as Polars
            print("\n✓ Test 2: Querying data as Polars DataFrame...")
            df = db.get_data_as_polars()
            print(f"  - Retrieved {len(df):,} records")
            print(f"  - Columns: {', '.join(df.columns[:5])}...")
            
            # Test 3: Custom query
            print("\n✓ Test 3: Executing custom query...")
            result = db.execute_query("SELECT COUNT(*) FROM ring_records")
            count = result.fetchone()[0]
            print(f"  - Record count: {count:,}")
            
            print("\n✅ Database tests passed!")
            return True
            
    except Exception as e:
        print(f"\n❌ Database test failed: {e}")
        return False


def test_data_processor():
    """Test data processing utilities."""
    print("\n" + "="*60)
    print("Testing Data Processing")
    print("="*60)
    
    csv_path = Path(__file__).parent / "data" / "processed" / "processed_nidingen_data.csv"
    
    if not csv_path.exists():
        print("❌ CSV file not found.")
        return False
    
    try:
        processor = BirdDataProcessor()
        
        # Test 1: Load CSV
        print("\n✓ Test 1: Loading CSV with Polars...")
        df = processor.load_csv(csv_path)
        print(f"  - Loaded {len(df):,} records")
        
        # Test 2: Add time features
        print("\n✓ Test 2: Adding time features...")
        df = processor.add_time_features(df)
        print(f"  - Added columns: year, month, day, season, etc.")
        
        # Test 3: Get species summary
        print("\n✓ Test 3: Generating species summary...")
        summary = processor.get_species_summary(df)
        print(f"  - Summarized {len(summary)} species")
        top_species = summary.head(3)
        print("\n  Top 3 species:")
        for row in top_species.iter_rows(named=True):
            print(f"    - {row['species_code']}: {row['total_records']:,} records")
        
        # Test 4: Filter by species
        print("\n✓ Test 4: Filtering by species...")
        filtered = processor.filter_by_species(df, ["GÄSMY", "BLMES"])
        print(f"  - Filtered to {len(filtered):,} records")
        
        print("\n✅ Data processing tests passed!")
        return True
        
    except Exception as e:
        print(f"\n❌ Data processing test failed: {e}")
        return False


def test_queries():
    """Test pre-built queries."""
    print("\n" + "="*60)
    print("Testing Pre-built Queries")
    print("="*60)
    
    db_path = Path(__file__).parent / "data" / "bird_ringing.db"
    
    try:
        # Test 1: Time series query
        print("\n✓ Test 1: Time series query...")
        query = BirdRingingQueries.get_species_time_series(
            species_codes=["GÄSMY"],
            aggregation="monthly"
        )
        print(f"  - Generated query: {len(query)} characters")
        
        with BirdRingingDB(db_path, read_only=True) as db:
            result = db.execute_query(query).pl()
            print(f"  - Retrieved {len(result)} monthly records")
        
        # Test 2: Phenology query
        print("\n✓ Test 2: Phenology query...")
        query = BirdRingingQueries.get_phenology_by_species(
            species_codes=["GÄSMY"],
            start_year=2020
        )
        with BirdRingingDB(db_path, read_only=True) as db:
            result = db.execute_query(query).pl()
            print(f"  - Retrieved phenology data for {len(result)} year-species combinations")
        
        # Test 3: Recapture analysis
        print("\n✓ Test 3: Recapture analysis...")
        query = BirdRingingQueries.get_recapture_analysis()
        with BirdRingingDB(db_path, read_only=True) as db:
            result = db.execute_query(query).pl()
            if len(result) > 0:
                print(f"  - Found {len(result)} recaptured birds")
                max_recaptures = result["n_captures"].max()
                print(f"  - Max recaptures for single bird: {max_recaptures}")
            else:
                print("  - No recaptures found in database")
        
        print("\n✅ Query tests passed!")
        return True
        
    except Exception as e:
        print(f"\n❌ Query test failed: {e}")
        return False


def main():
    """Run all tests."""
    print("\n" + "="*60)
    print("🧪 Bird Ringing Database - Test Suite")
    print("="*60)
    
    results = []
    
    # Run tests
    results.append(("Database Operations", test_database()))
    results.append(("Data Processing", test_data_processor()))
    results.append(("Pre-built Queries", test_queries()))
    
    # Summary
    print("\n" + "="*60)
    print("Test Summary")
    print("="*60)
    
    for test_name, passed in results:
        status = "✅ PASSED" if passed else "❌ FAILED"
        print(f"{test_name}: {status}")
    
    all_passed = all(result[1] for result in results)
    
    if all_passed:
        print("\n🎉 All tests passed! Your setup is working correctly.")
        print("\nNext steps:")
        print("1. Run the dashboard: python app.py")
        print("2. Open browser to: http://localhost:8050")
        print("3. Explore the data!")
    else:
        print("\n⚠️ Some tests failed. Please check the errors above.")
    
    print("="*60 + "\n")


if __name__ == "__main__":
    main()
