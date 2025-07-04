#!/usr/bin/env python3
"""
Check system resources to understand if OOM occurred.
"""

import os
import psutil
import subprocess

def main():
    """Check system resources."""
    
    print("=" * 60)
    print("SYSTEM RESOURCES CHECK")
    print("=" * 60)
    
    # Memory info
    memory = psutil.virtual_memory()
    print(f"Total RAM: {memory.total / (1024**3):.2f} GB")
    print(f"Available RAM: {memory.available / (1024**3):.2f} GB")
    print(f"Used RAM: {memory.used / (1024**3):.2f} GB")
    print(f"Memory usage: {memory.percent}%")
    
    # Disk info
    disk = psutil.disk_usage('.')
    print(f"\nDisk space:")
    print(f"Total: {disk.total / (1024**3):.2f} GB")
    print(f"Used: {disk.used / (1024**3):.2f} GB")
    print(f"Free: {disk.free / (1024**3):.2f} GB")
    print(f"Usage: {disk.percent}%")
    
    # Check for OOM in system logs
    print(f"\nChecking for OOM in system logs...")
    try:
        result = subprocess.run(['dmesg', '-T'], capture_output=True, text=True, timeout=10)
        if 'Out of memory' in result.stdout or 'oom' in result.stdout.lower():
            print("⚠️  OOM detected in system logs!")
            # Show last few OOM entries
            lines = result.stdout.split('\n')
            oom_lines = [line for line in lines if 'out of memory' in line.lower() or 'oom' in line.lower()]
            for line in oom_lines[-5:]:  # Last 5 OOM entries
                print(f"  {line}")
        else:
            print("✅ No OOM detected in recent system logs")
    except Exception as e:
        print(f"Could not check system logs: {e}")
    
    # Check if swap is enabled
    try:
        result = subprocess.run(['swapon', '--show'], capture_output=True, text=True)
        if result.stdout.strip():
            print(f"\n✅ Swap is enabled:")
            print(result.stdout)
        else:
            print(f"\n❌ No swap configured")
    except Exception as e:
        print(f"Could not check swap: {e}")

if __name__ == "__main__":
    main() 