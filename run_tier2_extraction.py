#!/usr/bin/env python3
"""
Runner script for Tier 2 Data Extraction
Executes the massive performance data extraction for ~135,000 records
"""

import subprocess
import sys
import time
from datetime import datetime

def main():
    print("="*80)
    print("TIER 2 DATA EXTRACTION - PERFORMANCE STATISTICS")
    print("="*80)
    print(f"Start time: {datetime.now()}")
    print("Expected: ~135,000 records across 5 performance tables")
    print("Target years: 2019-2025")
    print("-"*80)
    
    # Confirm execution
    response = input("\nThis will extract and insert ~135,000 records. Continue? (yes/no): ")
    if response.lower() != 'yes':
        print("Extraction cancelled.")
        return
    
    print("\nStarting extraction process...")
    print("This may take 1-2 hours to complete.")
    print("Progress will be displayed every 50 files.\n")
    
    start_time = time.time()
    
    # Run the extraction script
    try:
        result = subprocess.run(
            [sys.executable, 'extract_tier2_data.py'],
            capture_output=False,  # Show output in real-time
            text=True
        )
        
        if result.returncode == 0:
            elapsed = time.time() - start_time
            print("\n" + "="*80)
            print("EXTRACTION COMPLETED SUCCESSFULLY")
            print(f"Total time: {elapsed/60:.2f} minutes")
            print("Check tier2_extraction_report.txt for detailed results")
            print("="*80)
        else:
            print("\n" + "="*80)
            print("EXTRACTION FAILED")
            print(f"Return code: {result.returncode}")
            print("Check tier2_extraction.log for error details")
            print("="*80)
            
    except KeyboardInterrupt:
        print("\n\nExtraction interrupted by user.")
        print("Partial data may have been inserted.")
        print("Check tier2_extraction.log for details.")
    except Exception as e:
        print(f"\nError running extraction: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()