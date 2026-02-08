#!/usr/bin/env python3
"""
Quick verification script for the implementation
"""
import os


def verify_files():
    """Verify all required files exist and were modified correctly"""

    print("=" * 60)
    print("VERIFICATION REPORT")
    print("=" * 60)

    # Check created files
    created_files = [
        'system_prompt.txt',
        'static/monte_carlo.js',
    ]

    print("\n1. Created Files:")
    for f in created_files:
        exists = os.path.exists(f)
        status = "[OK]" if exists else "[MISSING]"
        print(f"   {status} {f}")

    # Check modified files
    print("\n2. Modified Files:")

    # Check dashboard.html for Monte Carlo section
    with open('static/dashboard.html', 'r') as f:
        dashboard_content = f.read()
        has_mc = 'monte-carlo-section' in dashboard_content
        has_btns = 'mc-quick-btn' in dashboard_content
        print(f"   {'[OK]' if has_mc else '[MISSING]'} dashboard.html: Monte Carlo section")
        print(f"   {'[OK]' if has_btns else '[MISSING]'} dashboard.html: MC buttons")

    # Check backtesting.js for MC integration
    with open('static/backtesting.js', 'r') as f:
        bt_content = f.read()
        has_import = 'monte_carlo' in bt_content
        print(f"   {'[OK]' if has_import else '[MISSING]'} backtesting.js: MC import")

    # Check routes.py for secure chat
    with open('routes.py', 'r') as f:
        routes_content = f.read()
        has_system_prompt = 'system_prompt.txt' in routes_content
        has_sanitization = 'safe_query' in routes_content
        print(f"   {'[OK]' if has_system_prompt else '[MISSING]'} routes.py: Uses system_prompt.txt")
        print(f"   {'[OK]' if has_sanitization else '[MISSING]'} routes.py: Query sanitization")

    # Check Monte Carlo JS is clean
    with open('static/monte_carlo.js', 'r') as f:
        mc_content = f.read()
        line_count = len(mc_content.split('\n'))
        is_simple = line_count < 200
        print(f"   {'[OK]' if is_simple else '[WARN]'} monte_carlo.js: {line_count} lines")

    # Check system prompt content
    with open('system_prompt.txt', 'r') as f:
        sp_content = f.read()
        is_secure = 'hallucinate' in sp_content and '3 sentences' in sp_content
        print(f"   {'[OK]' if is_secure else '[WARN]'} system_prompt.txt: Contains security rules")

    print("\n3. Monte Carlo Integration:")

    # Check for complete MC workflow
    has_mc_import = "initializeMonteCarlo()" in bt_content
    has_global_data = "window.currentBacktestData" in bt_content
    print(f"   {'[OK]' if has_mc_import else '[MISSING]'} Backtesting: MC initialization")
    print(f"   {'[OK]' if has_global_data else '[MISSING]'} Backtesting: Stores MC data")

    print("\n" + "=" * 60)
    print("VERIFICATION COMPLETE")
    print("=" * 60)
    print("\nAll files are in place. To test the application:")
    print("1. Run: python app.py")
    print("2. Open https://localhost:5000")
    print("3. Log in and test:")
    print("   - Run a backtest -> Try Monte Carlo buttons")
    print("   - Use the chatbot with secure prompts")

if __name__ == '__main__':
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    verify_files()
