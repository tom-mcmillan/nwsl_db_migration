#!/usr/bin/env python3
"""
Run the full Tier 1 extraction
"""

from extract_tier1_data import FBrefDataExtractor
import logging
import sys

# Set up logging
logging.basicConfig(level=logging.INFO)

def main():
    """Run extraction with limited scope first"""
    
    # Parse arguments
    limit = None
    if len(sys.argv) > 1:
        limit = int(sys.argv[1])
        print(f"Running extraction on first {limit} files")
    else:
        print("Running full extraction on all files")
    
    extractor = FBrefDataExtractor()
    
    try:
        # Connect to database
        if not extractor.connect_db():
            print("Failed to connect to database")
            return
        
        # Load cache
        extractor.load_cache()
        
        print(f"\nCache loaded:")
        print(f"  Teams: {len(extractor.cache['teams'])}")
        print(f"  Players: {len(extractor.cache['players'])}")  
        print(f"  Matches: {len(extractor.cache['matches'])}")
        
        # Get current counts
        extractor.cursor.execute("SELECT COUNT(*) as count FROM match_goalkeeper_summary")
        gk_before = extractor.cursor.fetchone()['count']
        
        extractor.cursor.execute("SELECT COUNT(*) as count FROM match_shot")
        shot_before = extractor.cursor.fetchone()['count']
        
        print(f"\nBefore extraction:")
        print(f"  Goalkeeper records: {gk_before}")
        print(f"  Shot records: {shot_before}")
        
        # Process files
        if limit:
            # Process limited number
            import os
            html_dir = '/Users/thomasmcmillan/projects/nwsl_data_backup_data/notebooks/match_html_files/'
            html_files = [f for f in os.listdir(html_dir) if f.endswith('.html')][:limit]
            
            for filename in html_files:
                filepath = os.path.join(html_dir, filename)
                match_hex_id = extractor.extract_match_id_from_filename(filename)
                
                if not match_hex_id or match_hex_id not in extractor.cache['matches']:
                    continue
                
                soup = extractor.parse_html_file(filepath)
                if not soup:
                    continue
                
                # Extract and insert goalkeeper data
                gk_data = extractor.extract_goalkeeper_data(soup, match_hex_id)
                if gk_data:
                    extractor.stats['goalkeeper']['extracted'] += len(gk_data)
                    inserted = extractor.insert_goalkeeper_records(gk_data)
                    extractor.stats['goalkeeper']['inserted'] += inserted
                
                # Extract and insert shot data (focus on recent years)
                extractor.cursor.execute("""
                    SELECT EXTRACT(YEAR FROM match_date) as year 
                    FROM match 
                    WHERE match_id = %s
                """, (match_hex_id,))
                result = extractor.cursor.fetchone()
                
                if result and result['year'] and int(result['year']) >= 2022:
                    shot_data = extractor.extract_shot_data(soup, match_hex_id)
                    if shot_data:
                        extractor.stats['shots']['extracted'] += len(shot_data)
                        inserted = extractor.insert_shot_records(shot_data)
                        extractor.stats['shots']['inserted'] += inserted
                
                extractor.stats['goalkeeper']['processed'] += 1
                extractor.stats['shots']['processed'] += 1
        else:
            # Run full extraction
            extractor.process_all_files(focus_years=[2022, 2023, 2024, 2025])
        
        # Get final counts
        extractor.cursor.execute("SELECT COUNT(*) as count FROM match_goalkeeper_summary")
        gk_after = extractor.cursor.fetchone()['count']
        
        extractor.cursor.execute("SELECT COUNT(*) as count FROM match_shot")
        shot_after = extractor.cursor.fetchone()['count']
        
        # Generate report
        print("\n" + "="*60)
        print("EXTRACTION COMPLETE")
        print("="*60)
        print(f"\nGOALKEEPER DATA:")
        print(f"  Files processed: {extractor.stats['goalkeeper']['processed']}")
        print(f"  Records extracted: {extractor.stats['goalkeeper']['extracted']}")
        print(f"  Records inserted: {extractor.stats['goalkeeper']['inserted']}")
        print(f"  Before: {gk_before} → After: {gk_after} (+"f"{gk_after - gk_before})")
        
        print(f"\nSHOT DATA:")
        print(f"  Files processed: {extractor.stats['shots']['processed']}")
        print(f"  Records extracted: {extractor.stats['shots']['extracted']}")
        print(f"  Records inserted: {extractor.stats['shots']['inserted']}")
        print(f"  Before: {shot_before} → After: {shot_after} (+"f"{shot_after - shot_before})")
        
        if extractor.stats['goalkeeper']['errors'] > 0:
            print(f"\nErrors encountered: {extractor.stats['goalkeeper']['errors']}")
        
        # Save full report
        report = extractor.generate_report()
        with open('tier1_extraction_report.txt', 'w') as f:
            f.write(report)
        print("\nFull report saved to: tier1_extraction_report.txt")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        extractor.close_db()

if __name__ == "__main__":
    main()