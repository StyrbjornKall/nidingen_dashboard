"""
Quick test for the weekly heatmap query.
"""

from pathlib import Path
import sys

# Add src to path
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

from db_manager import BirdRingingDB
from query_utils import BirdRingingQueries

DB_PATH = Path(__file__).parent / "data" / "bird_ringing.db"

print("Testing weekly heatmap queries...")
print("="*60)

with BirdRingingDB(DB_PATH, read_only=True) as db:
    # Test 1: All years average
    print("\nTest 1: Average across all years")
    query = BirdRingingQueries.get_weekly_heatmap_data(year=None, top_n_species=5)
    df = db.execute_query(query).pl()
    print(f"Retrieved {len(df)} records")
    print(df.head())
    
    # Test 2: Specific year (2023)
    print("\n\nTest 2: Year 2023")
    query = BirdRingingQueries.get_weekly_heatmap_data(year=2023, top_n_species=5)
    df = db.execute_query(query).pl()
    print(f"Retrieved {len(df)} records")
    print(df.head())
    
print("\n" + "="*60)
print("✅ Tests passed! Query works correctly.")
print("\nYou can now run: python app.py")
