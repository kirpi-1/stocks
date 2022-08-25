import subprocess
import os
files = os.listdir('distributions')
files = [f for f in files if 'distributions' in f]
for f in files:
    subprocess.run(['python','generate_distribution_plots.py', f"distributions/{f}"])
