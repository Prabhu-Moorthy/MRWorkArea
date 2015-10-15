This program was written as a solution for the question asked at StackOverflow

Original Question
------------------
I have two csv files. One has (user_id, gameName,score) and other has (user_id, age). 
How do I perform a join using map reduce programming so that I can calculate average age of players for each game. 
I have no idea how to proceed with this one.

Link to the original Question 
-----------------------------
http://stackoverflow.com/questions/32778822/joins-using-map-reduce-programming

Sample Input files:
GameFile.csv
------------
11,Mario,100
11,Contra,97
11,Tank,86
22,Contra,50
22,Age of Empire,56
33,Tank,76
33,Contra,89

Age.csv
-------
11,18
22,30
33,26

Output of the program for the above input
-----------------------------------------
Age of Empire	30
Contra	24
Mario	18
Tank	22
