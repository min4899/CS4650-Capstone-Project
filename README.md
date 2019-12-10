# CS4650-Capstone-Project
Capstone Project using XSEDE and Spark for Lan Yang's CS4650 class, by Minwoo Soh and Salvatore Grillo

Instructions:
1. Place the 'AppstoreMapReduce.py' and 'appstore_games.cv' files into XSEDE bridge using SSH/WinSCP
2. Log into XSEDE bridge (Host Name: bridges.psc.edu | Port: 2222) using SSH/PuTTY.
3. Request for resources: [ interact -N 4 -t 01:00:00 ] 
4. Load Spark module using: [ module load spark ] 
5. Start the program (make sure the mapreduce program and csv file are in the same directory): 
    [ spark-submit AppstoreMapReduce.py ]
