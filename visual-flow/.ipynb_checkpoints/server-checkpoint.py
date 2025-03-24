import subprocess

if __name__ == "__main__":
    subprocess.run(["streamlit", "run", "visualisation.py", "--server.port=8080", "--server.address=0.0.0.0"])
