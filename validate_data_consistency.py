#!/usr/bin/env python3
"""
Data Consistency Validation Script for NWSL Database
======================================================
Purpose: Validate data consistency between match, match_team_performance, and match_shot tables
Author: Database Migration Specialist
Date: 2025-08-12

This script performs comprehensive validation checks and generates reports on:
1. xG consistency across tables
2. Goal consistency between match and team performance tables
3. Completeness of team performance records
4. Data quality metrics
"""

import psycopg2
import json
from datetime import datetime
from typing import Dict, List, Tuple, Any
import sys

# Database connection parameters
DB_CONFIG = {
    'host': 'localhost',
    'port': 5433,
    'database': 'nwsl_data',
    'user': 'postgres',
    'password': 'postgres'
}

class DataConsistencyValidator:
    """Validates data consistency across NWSL database tables"""
    
    def __init__(self, db_config: Dict[str, Any]):
        """Initialize validator with database configuration"""
        self.db_config = db_config
        self.conn = None
        self.cur = None
        self.validation_results = {
            'timestamp': datetime.now().isoformat(),
            'checks': [],
            'summary': {},
            'issues': []
        }
    
    def connect(self):
        """Establish database connection"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.cur = self.conn.cursor()
            print("✓ Connected to database")
            return True
        except Exception as e:
            print(f"✗ Failed to connect to database: {e}")
            return False
    
    def disconnect(self):
        """Close database connection"""
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
        print("✓ Disconnected from database")
    
    def check_xg_consistency(self) -> Dict[str, Any]:
        """Check xG consistency across match, match_team_performance, and match_shot tables"""
        print("\n🔍 Checking xG consistency...")
        
        query = """
        WITH shot_xg AS (
            SELECT 
                ms.match_id,
                ms.team_name,
                SUM(ms.xg)::numeric(4,2) as shot_xg,
                COUNT(*) as shot_count
            FROM match_shot ms
            WHERE ms.xg IS NOT NULL
            GROUP BY ms.match_id, ms.team_name
        ),
        match_data AS (
            SELECT 
                m.match_id,
                hts.team_name_season_1 as home_team_name,
                ats.team_name_season_1 as away_team_name,
                m.xg_home,
                m.xg_away,
                home_mtp.xg as home_team_perf_xg,
                away_mtp.xg as away_team_perf_xg
            FROM match m
            LEFT JOIN match_team_performance home_mtp 
                ON m.match_id = home_mtp.match_id AND home_mtp.is_home = true
            LEFT JOIN match_team_performance away_mtp 
                ON m.match_id = away_mtp.match_id AND away_mtp.is_home = false
            LEFT JOIN team_season hts ON m.home_team_season_id = hts.id
            LEFT JOIN team_season ats ON m.away_team_season_id = ats.id
        )
        SELECT 
            md.match_id,
            md.home_team_name,
            md.away_team_name,
            md.xg_home,
            home_shot.shot_xg as home_shot_xg,
            md.home_team_perf_xg,
            md.xg_away,
            away_shot.shot_xg as away_shot_xg,
            md.away_team_perf_xg,
            CASE 
                WHEN md.xg_home = home_shot.shot_xg 
                    AND md.xg_home = md.home_team_perf_xg 
                    AND md.xg_away = away_shot.shot_xg 
                    AND md.xg_away = md.away_team_perf_xg 
                THEN 'CONSISTENT'
                WHEN home_shot.shot_xg IS NULL AND away_shot.shot_xg IS NULL
                THEN 'NO_SHOT_DATA'
                ELSE 'INCONSISTENT'
            END as status
        FROM match_data md
        LEFT JOIN shot_xg home_shot 
            ON md.match_id = home_shot.match_id 
            AND md.home_team_name = home_shot.team_name
        LEFT JOIN shot_xg away_shot 
            ON md.match_id = away_shot.match_id 
            AND md.away_team_name = away_shot.team_name
        """
        
        self.cur.execute(query)
        results = self.cur.fetchall()
        
        # Analyze results
        total_matches = len(results)
        consistent = sum(1 for r in results if r[9] == 'CONSISTENT')
        inconsistent = sum(1 for r in results if r[9] == 'INCONSISTENT')
        no_shot_data = sum(1 for r in results if r[9] == 'NO_SHOT_DATA')
        
        # Collect sample inconsistencies
        inconsistencies = []
        for row in results:
            if row[9] == 'INCONSISTENT':
                inconsistencies.append({
                    'match_id': row[0],
                    'home_team': row[1],
                    'away_team': row[2],
                    'match_xg': f"{row[3]}/{row[6]}",
                    'shot_xg': f"{row[4]}/{row[7]}",
                    'team_perf_xg': f"{row[5]}/{row[8]}"
                })
                if len(inconsistencies) >= 5:  # Limit to 5 samples
                    break
        
        result = {
            'check': 'xG Consistency',
            'total_matches': total_matches,
            'consistent': consistent,
            'inconsistent': inconsistent,
            'no_shot_data': no_shot_data,
            'consistency_rate': round(100 * consistent / total_matches, 2) if total_matches > 0 else 0,
            'sample_issues': inconsistencies
        }
        
        print(f"  ✓ Total matches: {total_matches}")
        print(f"  ✓ Consistent: {consistent} ({result['consistency_rate']}%)")
        print(f"  ⚠ Inconsistent: {inconsistent}")
        print(f"  ℹ No shot data: {no_shot_data}")
        
        return result
    
    def check_goal_consistency(self) -> Dict[str, Any]:
        """Check goal consistency between match and match_team_performance tables"""
        print("\n🔍 Checking goal consistency...")
        
        query = """
        SELECT 
            m.match_id,
            m.home_goals as match_home_goals,
            m.away_goals as match_away_goals,
            home_mtp.goals as team_perf_home_goals,
            away_mtp.goals as team_perf_away_goals,
            home_mtp.goals_against as home_goals_against,
            away_mtp.goals_against as away_goals_against,
            CASE 
                WHEN m.home_goals = home_mtp.goals 
                    AND m.away_goals = away_mtp.goals
                    AND m.home_goals = away_mtp.goals_against
                    AND m.away_goals = home_mtp.goals_against
                THEN 'CONSISTENT'
                WHEN home_mtp.goals IS NULL OR away_mtp.goals IS NULL
                THEN 'MISSING_DATA'
                ELSE 'INCONSISTENT'
            END as status
        FROM match m
        LEFT JOIN match_team_performance home_mtp 
            ON m.match_id = home_mtp.match_id AND home_mtp.is_home = true
        LEFT JOIN match_team_performance away_mtp 
            ON m.match_id = away_mtp.match_id AND away_mtp.is_home = false
        """
        
        self.cur.execute(query)
        results = self.cur.fetchall()
        
        # Analyze results
        total_matches = len(results)
        consistent = sum(1 for r in results if r[7] == 'CONSISTENT')
        inconsistent = sum(1 for r in results if r[7] == 'INCONSISTENT')
        missing_data = sum(1 for r in results if r[7] == 'MISSING_DATA')
        
        # Collect sample inconsistencies
        inconsistencies = []
        for row in results:
            if row[7] == 'INCONSISTENT':
                inconsistencies.append({
                    'match_id': row[0],
                    'match_goals': f"{row[1]}-{row[2]}",
                    'team_perf_goals': f"{row[3]}-{row[4]}",
                    'goals_against': f"{row[5]}/{row[6]}"
                })
                if len(inconsistencies) >= 5:
                    break
        
        result = {
            'check': 'Goal Consistency',
            'total_matches': total_matches,
            'consistent': consistent,
            'inconsistent': inconsistent,
            'missing_data': missing_data,
            'consistency_rate': round(100 * consistent / total_matches, 2) if total_matches > 0 else 0,
            'sample_issues': inconsistencies
        }
        
        print(f"  ✓ Total matches: {total_matches}")
        print(f"  ✓ Consistent: {consistent} ({result['consistency_rate']}%)")
        print(f"  ⚠ Inconsistent: {inconsistent}")
        print(f"  ℹ Missing data: {missing_data}")
        
        return result
    
    def check_team_record_completeness(self) -> Dict[str, Any]:
        """Check that each match has exactly 2 team performance records"""
        print("\n🔍 Checking team record completeness...")
        
        query = """
        SELECT 
            m.match_id,
            m.match_date,
            COUNT(mtp.id) as team_record_count,
            CASE 
                WHEN COUNT(mtp.id) = 2 THEN 'COMPLETE'
                WHEN COUNT(mtp.id) = 1 THEN 'ONE_RECORD'
                WHEN COUNT(mtp.id) = 0 THEN 'NO_RECORDS'
                ELSE 'TOO_MANY_RECORDS'
            END as status
        FROM match m
        LEFT JOIN match_team_performance mtp ON m.match_id = mtp.match_id
        GROUP BY m.match_id, m.match_date
        """
        
        self.cur.execute(query)
        results = self.cur.fetchall()
        
        # Analyze results
        total_matches = len(results)
        complete = sum(1 for r in results if r[3] == 'COMPLETE')
        one_record = sum(1 for r in results if r[3] == 'ONE_RECORD')
        no_records = sum(1 for r in results if r[3] == 'NO_RECORDS')
        too_many = sum(1 for r in results if r[3] == 'TOO_MANY_RECORDS')
        
        # Collect sample issues
        issues = []
        for row in results:
            if row[3] != 'COMPLETE':
                issues.append({
                    'match_id': row[0],
                    'match_date': row[1].isoformat() if row[1] else None,
                    'record_count': row[2],
                    'status': row[3]
                })
                if len(issues) >= 5:
                    break
        
        result = {
            'check': 'Team Record Completeness',
            'total_matches': total_matches,
            'complete': complete,
            'one_record': one_record,
            'no_records': no_records,
            'too_many_records': too_many,
            'completeness_rate': round(100 * complete / total_matches, 2) if total_matches > 0 else 0,
            'sample_issues': issues
        }
        
        print(f"  ✓ Total matches: {total_matches}")
        print(f"  ✓ Complete (2 records): {complete} ({result['completeness_rate']}%)")
        print(f"  ⚠ One record only: {one_record}")
        print(f"  ⚠ No records: {no_records}")
        print(f"  ⚠ Too many records: {too_many}")
        
        return result
    
    def check_null_xg_values(self) -> Dict[str, Any]:
        """Check for NULL xG values in match_team_performance where shot data exists"""
        print("\n🔍 Checking for NULL xG values...")
        
        query = """
        WITH teams_with_shots AS (
            SELECT DISTINCT
                ms.match_id,
                ms.team_name,
                SUM(ms.xg)::numeric(4,2) as calculated_xg
            FROM match_shot ms
            WHERE ms.xg IS NOT NULL
            GROUP BY ms.match_id, ms.team_name
        )
        SELECT 
            mtp.match_id,
            ts.team_name_season_1,
            mtp.xg as current_xg,
            tws.calculated_xg,
            mtp.is_home
        FROM match_team_performance mtp
        JOIN team_season ts ON mtp.team_season_id = ts.id
        JOIN teams_with_shots tws 
            ON mtp.match_id = tws.match_id 
            AND (ts.team_name_season_1 = tws.team_name OR ts.team_name_season_2 = tws.team_name)
        WHERE mtp.xg IS NULL
        """
        
        self.cur.execute(query)
        results = self.cur.fetchall()
        
        null_count = len(results)
        
        # Collect sample issues
        issues = []
        for row in results[:5]:
            issues.append({
                'match_id': row[0],
                'team': row[1],
                'calculated_xg': float(row[3]) if row[3] else None,
                'is_home': row[4]
            })
        
        result = {
            'check': 'NULL xG Values',
            'null_xg_with_shot_data': null_count,
            'sample_issues': issues
        }
        
        print(f"  ⚠ NULL xG values with shot data: {null_count}")
        
        return result
    
    def check_shot_goal_consistency(self) -> Dict[str, Any]:
        """Check if goals in match_shot align with recorded goals"""
        print("\n🔍 Checking shot-goal consistency...")
        
        query = """
        WITH shot_goals AS (
            SELECT 
                match_id,
                team_name,
                COUNT(CASE WHEN outcome = 'Goal' THEN 1 END) as goals_from_shots
            FROM match_shot
            GROUP BY match_id, team_name
        ),
        match_goals AS (
            SELECT 
                m.match_id,
                hts.team_name_season_1 as home_team_name,
                ats.team_name_season_1 as away_team_name,
                m.home_goals,
                m.away_goals
            FROM match m
            LEFT JOIN team_season hts ON m.home_team_season_id = hts.id
            LEFT JOIN team_season ats ON m.away_team_season_id = ats.id
        )
        SELECT 
            mg.match_id,
            mg.home_team_name,
            mg.home_goals,
            COALESCE(home_sg.goals_from_shots, 0) as home_shot_goals,
            mg.away_team_name,
            mg.away_goals,
            COALESCE(away_sg.goals_from_shots, 0) as away_shot_goals,
            CASE 
                WHEN mg.home_goals = COALESCE(home_sg.goals_from_shots, 0)
                    AND mg.away_goals = COALESCE(away_sg.goals_from_shots, 0)
                THEN 'CONSISTENT'
                ELSE 'INCONSISTENT'
            END as status
        FROM match_goals mg
        LEFT JOIN shot_goals home_sg 
            ON mg.match_id = home_sg.match_id 
            AND mg.home_team_name = home_sg.team_name
        LEFT JOIN shot_goals away_sg 
            ON mg.match_id = away_sg.match_id 
            AND mg.away_team_name = away_sg.team_name
        WHERE home_sg.goals_from_shots IS NOT NULL 
            OR away_sg.goals_from_shots IS NOT NULL
        """
        
        self.cur.execute(query)
        results = self.cur.fetchall()
        
        total = len(results)
        consistent = sum(1 for r in results if r[7] == 'CONSISTENT')
        inconsistent = total - consistent
        
        # Collect sample inconsistencies
        issues = []
        for row in results:
            if row[7] == 'INCONSISTENT':
                issues.append({
                    'match_id': row[0],
                    'home_team': row[1],
                    'recorded_home_goals': row[2],
                    'shot_home_goals': row[3],
                    'away_team': row[4],
                    'recorded_away_goals': row[5],
                    'shot_away_goals': row[6]
                })
                if len(issues) >= 5:
                    break
        
        result = {
            'check': 'Shot-Goal Consistency',
            'total_matches_with_shots': total,
            'consistent': consistent,
            'inconsistent': inconsistent,
            'consistency_rate': round(100 * consistent / total, 2) if total > 0 else 0,
            'sample_issues': issues
        }
        
        print(f"  ✓ Matches with shot data: {total}")
        print(f"  ✓ Consistent: {consistent} ({result['consistency_rate']}%)")
        print(f"  ⚠ Inconsistent: {inconsistent}")
        
        return result
    
    def generate_summary(self) -> Dict[str, Any]:
        """Generate overall summary of validation results"""
        print("\n📊 Generating summary...")
        
        # Calculate overall health score
        total_checks = len(self.validation_results['checks'])
        
        if total_checks > 0:
            avg_consistency = sum(
                check.get('consistency_rate', 0) 
                for check in self.validation_results['checks']
                if 'consistency_rate' in check
            ) / sum(1 for check in self.validation_results['checks'] if 'consistency_rate' in check)
        else:
            avg_consistency = 0
        
        # Count total issues
        total_issues = sum(
            check.get('inconsistent', 0) + 
            check.get('missing_data', 0) + 
            check.get('one_record', 0) + 
            check.get('no_records', 0) + 
            check.get('null_xg_with_shot_data', 0)
            for check in self.validation_results['checks']
        )
        
        summary = {
            'overall_health_score': round(avg_consistency, 2),
            'total_checks_performed': total_checks,
            'total_issues_found': total_issues,
            'recommendation': self._get_recommendation(avg_consistency, total_issues)
        }
        
        print(f"  ✓ Overall health score: {summary['overall_health_score']}%")
        print(f"  ✓ Total issues found: {summary['total_issues_found']}")
        print(f"  📝 Recommendation: {summary['recommendation']}")
        
        return summary
    
    def _get_recommendation(self, health_score: float, total_issues: int) -> str:
        """Get recommendation based on health score and issues"""
        if health_score >= 95 and total_issues < 10:
            return "Database is in excellent condition. Continue regular monitoring."
        elif health_score >= 90:
            return "Database is healthy but has minor issues. Run fix_xg_data_consistency.sql to resolve."
        elif health_score >= 80:
            return "Database has moderate issues. Review and fix inconsistencies using provided SQL script."
        else:
            return "Database has significant consistency issues. Immediate remediation required."
    
    def save_report(self, filename: str = None):
        """Save validation report to JSON file"""
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"data_consistency_report_{timestamp}.json"
        
        with open(filename, 'w') as f:
            json.dump(self.validation_results, f, indent=2, default=str)
        
        print(f"\n💾 Report saved to: {filename}")
        return filename
    
    def run_all_checks(self):
        """Run all validation checks"""
        print("=" * 60)
        print("NWSL DATABASE CONSISTENCY VALIDATION")
        print("=" * 60)
        
        if not self.connect():
            return False
        
        try:
            # Run all checks
            self.validation_results['checks'].append(self.check_xg_consistency())
            self.validation_results['checks'].append(self.check_goal_consistency())
            self.validation_results['checks'].append(self.check_team_record_completeness())
            self.validation_results['checks'].append(self.check_null_xg_values())
            self.validation_results['checks'].append(self.check_shot_goal_consistency())
            
            # Generate summary
            self.validation_results['summary'] = self.generate_summary()
            
            # Save report
            report_file = self.save_report()
            
            print("\n" + "=" * 60)
            print("VALIDATION COMPLETE")
            print("=" * 60)
            
            return True
            
        except Exception as e:
            print(f"\n✗ Error during validation: {e}")
            return False
            
        finally:
            self.disconnect()

def main():
    """Main execution function"""
    validator = DataConsistencyValidator(DB_CONFIG)
    
    if validator.run_all_checks():
        sys.exit(0)
    else:
        sys.exit(1)

if __name__ == "__main__":
    main()