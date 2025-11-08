"""
Data Quality Checks for ETL Pipeline
Validates data integrity before and after loading
"""

import pandas as pd
import logging
from typing import Dict, List, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)


class DataQualityChecker:
    """Data quality validation and reporting"""
    
    def __init__(self):
        self.quality_report = {}
    
    def validate_dataframe(self, df: pd.DataFrame, table_name: str) -> Dict:
        """
        Comprehensive data quality validation
        
        Args:
            df: DataFrame to validate
            table_name: Name of the table
            
        Returns:
            Dictionary with validation results
        """
        issues = []
        warnings = []
        stats = {}
        
        # 1. Basic stats
        stats['total_rows'] = len(df)
        stats['total_columns'] = len(df.columns)
        stats['memory_usage_mb'] = df.memory_usage(deep=True).sum() / 1024 / 1024
        
        # 2. Check for empty DataFrame
        if len(df) == 0:
            issues.append("DataFrame is empty")
            return {
                'table': table_name,
                'status': 'FAILED',
                'issues': issues,
                'warnings': warnings,
                'stats': stats
            }
        
        # 3. Check for duplicate rows
        duplicate_count = df.duplicated().sum()
        if duplicate_count > 0:
            duplicate_pct = (duplicate_count / len(df)) * 100
            warnings.append(f"Found {duplicate_count} duplicate rows ({duplicate_pct:.2f}%)")
            stats['duplicate_rows'] = duplicate_count
        
        # 4. Check NULL values per column
        null_stats = {}
        high_null_columns = []
        
        for col in df.columns:
            null_count = df[col].isnull().sum()
            if null_count > 0:
                null_pct = (null_count / len(df)) * 100
                null_stats[col] = {
                    'count': int(null_count),
                    'percentage': round(null_pct, 2)
                }
                
                # Flag columns with >80% NULL
                if null_pct > 80:
                    high_null_columns.append(f"{col} ({null_pct:.1f}%)")
        
        if high_null_columns:
            warnings.append(f"High NULL percentage in columns: {', '.join(high_null_columns)}")
        
        stats['null_values'] = null_stats
        
        # 5. Check data types
        dtype_stats = {}
        for col in df.columns:
            dtype_stats[col] = str(df[col].dtype)
        stats['data_types'] = dtype_stats
        
        # 6. Check for columns with single unique value (potential issues)
        single_value_cols = []
        for col in df.columns:
            if df[col].nunique() == 1:
                single_value_cols.append(col)
        
        if single_value_cols:
            warnings.append(f"Columns with single value: {', '.join(single_value_cols)}")
        
        # 7. Check numeric columns for outliers
        numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
        outlier_stats = {}
        
        for col in numeric_cols:
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            
            if IQR > 0:  # Avoid division by zero
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                
                outliers = df[(df[col] < lower_bound) | (df[col] > upper_bound)]
                if len(outliers) > 0:
                    outlier_pct = (len(outliers) / len(df)) * 100
                    outlier_stats[col] = {
                        'count': len(outliers),
                        'percentage': round(outlier_pct, 2)
                    }
        
        if outlier_stats:
            stats['outliers'] = outlier_stats
        
        # 8. Check string columns for suspicious patterns
        string_cols = df.select_dtypes(include=['object']).columns
        suspicious_patterns = {}
        
        for col in string_cols:
            # Check for common placeholder values
            placeholders = df[col].isin(['null', 'NULL', 'None', 'N/A', 'NA', '', ' ']).sum()
            if placeholders > 0:
                suspicious_patterns[col] = f"{placeholders} placeholder values"
        
        if suspicious_patterns:
            warnings.append(f"Suspicious patterns found: {suspicious_patterns}")
        
        # Determine overall status
        status = 'PASSED'
        if issues:
            status = 'FAILED'
        elif warnings:
            status = 'WARNING'
        
        return {
            'table': table_name,
            'status': status,
            'issues': issues,
            'warnings': warnings,
            'stats': stats,
            'timestamp': datetime.now().isoformat()
        }
    
    def compare_row_counts(self, src_count: int, dst_count: int, table_name: str) -> Dict:
        """
        Compare source and destination row counts
        
        Args:
            src_count: Source table row count
            dst_count: Destination table row count
            table_name: Name of the table
            
        Returns:
            Comparison result
        """
        match = src_count == dst_count
        difference = dst_count - src_count
        difference_pct = (difference / src_count * 100) if src_count > 0 else 0
        
        status = 'MATCH' if match else 'MISMATCH'
        
        result = {
            'table': table_name,
            'status': status,
            'source_rows': src_count,
            'destination_rows': dst_count,
            'difference': difference,
            'difference_percentage': round(difference_pct, 2),
            'timestamp': datetime.now().isoformat()
        }
        
        if not match:
            if difference > 0:
                result['message'] = f"Destination has {difference} more rows than source"
            else:
                result['message'] = f"Destination has {abs(difference)} fewer rows than source"
        
        return result
    
    def validate_primary_key(self, df: pd.DataFrame, pk_columns: List[str], table_name: str) -> Dict:
        """
        Validate primary key integrity
        
        Args:
            df: DataFrame to validate
            pk_columns: List of primary key column names
            table_name: Name of the table
            
        Returns:
            Validation result
        """
        issues = []
        
        if not pk_columns:
            return {
                'table': table_name,
                'status': 'NO_PK',
                'message': 'No primary key defined'
            }
        
        # Check if PK columns exist
        missing_cols = [col for col in pk_columns if col not in df.columns]
        if missing_cols:
            issues.append(f"Primary key columns missing: {', '.join(missing_cols)}")
        
        # Check for NULL values in PK columns
        for col in pk_columns:
            if col in df.columns:
                null_count = df[col].isnull().sum()
                if null_count > 0:
                    issues.append(f"NULL values in primary key column '{col}': {null_count}")
        
        # Check for duplicate PK values
        if all(col in df.columns for col in pk_columns):
            duplicate_count = df.duplicated(subset=pk_columns).sum()
            if duplicate_count > 0:
                issues.append(f"Duplicate primary key values: {duplicate_count}")
        
        status = 'PASSED' if not issues else 'FAILED'
        
        return {
            'table': table_name,
            'status': status,
            'primary_key': pk_columns,
            'issues': issues,
            'timestamp': datetime.now().isoformat()
        }
    
    def generate_quality_report(self, table_name: str, validations: List[Dict]) -> str:
        """
        Generate a formatted quality report
        
        Args:
            table_name: Name of the table
            validations: List of validation results
            
        Returns:
            Formatted report string
        """
        report = []
        report.append("=" * 80)
        report.append(f"DATA QUALITY REPORT: {table_name}")
        report.append("=" * 80)
        
        for validation in validations:
            report.append(f"\n[{validation.get('status', 'UNKNOWN')}] {validation.get('table', 'Unknown')}")
            
            if 'issues' in validation and validation['issues']:
                report.append("  Issues:")
                for issue in validation['issues']:
                    report.append(f"    ❌ {issue}")
            
            if 'warnings' in validation and validation['warnings']:
                report.append("  Warnings:")
                for warning in validation['warnings']:
                    report.append(f"    ⚠️  {warning}")
            
            if 'stats' in validation:
                stats = validation['stats']
                report.append("  Statistics:")
                report.append(f"    • Total rows: {stats.get('total_rows', 0):,}")
                report.append(f"    • Total columns: {stats.get('total_columns', 0)}")
                report.append(f"    • Memory usage: {stats.get('memory_usage_mb', 0):.2f} MB")
                
                if 'duplicate_rows' in stats:
                    report.append(f"    • Duplicate rows: {stats['duplicate_rows']:,}")
        
        report.append("=" * 80)
        return "\n".join(report)


# Singleton instance
quality_checker = DataQualityChecker()


def validate_before_load(df: pd.DataFrame, table_name: str, pk_columns: List[str] = None) -> bool:
    """
    Quick validation before loading data
    
    Args:
        df: DataFrame to validate
        table_name: Name of the table
        pk_columns: Primary key columns
        
    Returns:
        True if validation passed, False otherwise
    """
    try:
        # Basic validation
        validation = quality_checker.validate_dataframe(df, table_name)
        
        if validation['status'] == 'FAILED':
            logger.error(f"❌ Data quality check FAILED for {table_name}")
            for issue in validation['issues']:
                logger.error(f"  • {issue}")
            return False
        
        if validation['status'] == 'WARNING':
            logger.warning(f"⚠️  Data quality warnings for {table_name}")
            for warning in validation['warnings']:
                logger.warning(f"  • {warning}")
        
        # PK validation if provided
        if pk_columns:
            pk_validation = quality_checker.validate_primary_key(df, pk_columns, table_name)
            if pk_validation['status'] == 'FAILED':
                logger.error(f"❌ Primary key validation FAILED for {table_name}")
                for issue in pk_validation['issues']:
                    logger.error(f"  • {issue}")
                return False
        
        logger.info(f"✅ Data quality check PASSED for {table_name}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Data quality check error for {table_name}: {e}")
        return False
