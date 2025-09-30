#!/usr/bin/env python3
"""
Batch Textbook Import Script

This script imports all 52 textbooks from the textbooks directory using the 
load_textbooks.py script, but only outputs statistics to a log file.

Usage:
    python scripts/batch_import_textbooks.py
"""

import os
import sys
import subprocess
import time
from datetime import datetime
from pathlib import Path
import re

def get_textbook_directories():
    """Get all textbook directories from the textbooks folder."""
    textbooks_dir = Path("textbooks")
    if not textbooks_dir.exists():
        print("ERROR: textbooks directory not found")
        return []
    
    # Get all directories in textbooks folder, excluding template and playground
    textbook_dirs = []
    for item in textbooks_dir.iterdir():
        if item.is_dir() and not item.name.startswith("template") and item.name != "osbooks-playground":
            textbook_dirs.append(item.name)
    
    return sorted(textbook_dirs)

def filter_statistics_output(output):
    """Filter output to only include statistics and summary information."""
    lines = output.split('\n')
    filtered_lines = []
    
    # Patterns to match statistics and summary lines
    stats_patterns = [
        r'=== .*SUMMARY.*===',
        r'=== .*COMPLETE.*===',
        r'=== .*STATS.*===',
        r'Started:',
        r'Finished:',
        r'Total time:',
        r'Duration:',
        r'Collections processed:',
        r'Sentences processed:',
        r'Concepts created:',
        r'Entities extracted:',
        r'API calls made:',
        r'Cache hits:',
        r'Cache hit rate:',
        r'Success rate:',
        r'Loading completed!',
        r'Textbook:',
        r'Collection:',
        r'Database:',
        r'Neo4j Browser:',
        r'Navigate to',
        r'To clear existing data',
        r'Loading cancelled by user',
        r'Loading failed:',
        r'ERROR:',
        r'WARNING:',
        r'Found existing JSON files',
        r'JSON processing completed:',
        r'Force concept import completed:',
        r'Relationship creation completed:',
        r'Collections processed:',
        r'Concepts:',
        r'Relationships:',
        r'Fixed.*orphaned',
        r'Created.*missing',
        r'No orphaned nodes found',
        r'Initialized bulk importer',
        r'Loading collection:',
        r'Collection loading completed!',
        r'Force flag enabled',
        r'Processing JSON files'
    ]
    
    for line in lines:
        # Check if line matches any statistics pattern
        if any(re.search(pattern, line, re.IGNORECASE) for pattern in stats_patterns):
            filtered_lines.append(line)
        # Also include lines that contain numbers (likely statistics)
        elif re.search(r'\d+', line) and any(keyword in line.lower() for keyword in 
            ['created', 'processed', 'imported', 'extracted', 'calls', 'hits', 'rate', 'time', 'duration']):
            filtered_lines.append(line)
    
    return '\n'.join(filtered_lines)

def import_textbook(textbook_name, log_file):
    """Import a single textbook and log only statistics."""
    print(f"Starting import of {textbook_name}...")
    
    # Command to run
    cmd = [
        sys.executable, 
        "scripts/load_textbooks.py", 
        "--textbook-path", 
        f".\\textbooks\\{textbook_name}"
    ]
    
    try:
        # Run the command and capture output
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=os.getcwd()
        )
        
        # Get the output
        stdout = result.stdout
        stderr = result.stderr
        
        # Log the start of this textbook import
        log_file.write(f"\n{'='*80}\n")
        log_file.write(f"TEXTBOOK: {textbook_name}\n")
        log_file.write(f"STARTED: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        log_file.write(f"{'='*80}\n")
        
        # Filter and log only statistics
        if stdout:
            filtered_output = filter_statistics_output(stdout)
            if filtered_output.strip():
                log_file.write("STDOUT (Statistics Only):\n")
                log_file.write(filtered_output)
                log_file.write("\n")
        
        if stderr:
            # Log errors and warnings
            log_file.write("STDERR:\n")
            log_file.write(stderr)
            log_file.write("\n")
        
        # Log the return code
        log_file.write(f"RETURN CODE: {result.returncode}\n")
        log_file.write(f"COMPLETED: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        log_file.write(f"{'='*80}\n\n")
        
        # Flush to ensure it's written
        log_file.flush()
        
        if result.returncode == 0:
            print(f"✓ Successfully imported {textbook_name}")
        else:
            print(f"✗ Failed to import {textbook_name} (return code: {result.returncode})")
            
        return result.returncode == 0
        
    except Exception as e:
        print(f"✗ Error importing {textbook_name}: {e}")
        log_file.write(f"ERROR: {e}\n")
        log_file.flush()
        return False

def main():
    """Main function to import all textbooks."""
    print("Batch Textbook Import Script")
    print("=" * 50)
    
    # Get all textbook directories
    textbook_dirs = get_textbook_directories()
    
    if not textbook_dirs:
        print("No textbook directories found!")
        return
    
    print(f"Found {len(textbook_dirs)} textbooks to import:")
    for i, textbook in enumerate(textbook_dirs, 1):
        print(f"  {i:2d}. {textbook}")
    
    # Create log file with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"textbook_import_log_{timestamp}.txt"
    
    print(f"\nLogging statistics to: {log_filename}")
    print("Starting batch import...\n")
    
    # Track statistics
    successful_imports = 0
    failed_imports = 0
    start_time = time.time()
    
    with open(log_filename, 'w', encoding='utf-8') as log_file:
        # Write header
        log_file.write("BATCH TEXTBOOK IMPORT LOG\n")
        log_file.write(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        log_file.write(f"Total textbooks: {len(textbook_dirs)}\n")
        log_file.write("=" * 80 + "\n\n")
        
        # Import each textbook
        for i, textbook in enumerate(textbook_dirs, 1):
            print(f"[{i}/{len(textbook_dirs)}] Importing {textbook}...")
            
            success = import_textbook(textbook, log_file)
            
            if success:
                successful_imports += 1
            else:
                failed_imports += 1
            
            # Add a small delay between imports to avoid overwhelming the system
            if i < len(textbook_dirs):
                time.sleep(2)
    
    # Calculate total time
    end_time = time.time()
    total_time = end_time - start_time
    total_timedelta = datetime.fromtimestamp(end_time) - datetime.fromtimestamp(start_time)
    
    # Write final summary to log
    with open(log_filename, 'a', encoding='utf-8') as log_file:
        log_file.write("\n" + "=" * 80 + "\n")
        log_file.write("BATCH IMPORT SUMMARY\n")
        log_file.write("=" * 80 + "\n")
        log_file.write(f"Total textbooks: {len(textbook_dirs)}\n")
        log_file.write(f"Successful imports: {successful_imports}\n")
        log_file.write(f"Failed imports: {failed_imports}\n")
        log_file.write(f"Success rate: {successful_imports/len(textbook_dirs)*100:.1f}%\n")
        log_file.write(f"Started: {datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S')}\n")
        log_file.write(f"Finished: {datetime.fromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S')}\n")
        log_file.write(f"Total time: {total_timedelta}\n")
        log_file.write(f"Duration: {total_time:.2f} seconds\n")
        log_file.write("=" * 80 + "\n")
    
    # Print final summary
    print("\n" + "=" * 50)
    print("BATCH IMPORT COMPLETE")
    print("=" * 50)
    print(f"Total textbooks: {len(textbook_dirs)}")
    print(f"Successful imports: {successful_imports}")
    print(f"Failed imports: {failed_imports}")
    print(f"Success rate: {successful_imports/len(textbook_dirs)*100:.1f}%")
    print(f"Total time: {total_timedelta}")
    print(f"Log file: {log_filename}")

if __name__ == "__main__":
    main()
